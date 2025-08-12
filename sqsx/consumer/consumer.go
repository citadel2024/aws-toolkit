package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/citadel2024/aws-toolkit/sqsx"
)

// MessageHandlerFunc is a function type that defines how to handle incoming SQS messages.
// If the error is nil, the message is considered successfully processed.
// If the error wraps ErrNonRetryable, the message will be deleted and not retried.
// If the error is any other type, the message will be NACKed (made immediately visible again) for retry later.
type MessageHandlerFunc func(ctx context.Context, msg *types.Message) error

// ExceptionHandlerFunc is a function type that defines how to handle exceptions
type ExceptionHandlerFunc func(ctx context.Context, err error)

type Option func(c *sqsConsumer)

func WithMessageHandler(handler MessageHandlerFunc) Option {
	return func(c *sqsConsumer) {
		c.messageHandler = handler
	}
}

func WithExceptionHandler(handler ExceptionHandlerFunc) Option {
	return func(c *sqsConsumer) {
		c.exceptionHandler = handler
	}
}

func WithLogger(logger zerolog.Logger) Option {
	return func(c *sqsConsumer) {
		c.logger = logger
	}
}

func WithPollingGoroutines(count int) Option {
	return func(c *sqsConsumer) {
		if count > 0 {
			c.pollingConcurrency = count
		}
	}
}

func WithProcessingConcurrency(count int) Option {
	return func(c *sqsConsumer) {
		if count > 0 {
			c.processingConcurrency = count
		}
	}
}

func WithMaxMessagesPerBatch(count int32) Option {
	return func(c *sqsConsumer) {
		if count > 0 && count <= 10 {
			c.maxMessagesPerBatch = count
		}
	}
}

func WithWaitTimeSeconds(seconds int32) Option {
	return func(c *sqsConsumer) {
		if seconds >= 0 && seconds <= 20 {
			c.waitTimeSeconds = seconds
		}
	}
}

func WithShutdownHook(hook func()) Option {
	return func(c *sqsConsumer) {
		c.shutdownHook = hook
	}
}

type sqsConsumer struct {
	// queueURL is the SQS queue URL where messages will be sent.
	queueURL string
	// logger is used for logging events and errors. Default zerolog.Nop()
	logger zerolog.Logger
	// client is the SQS client used to send messages.
	client Client
	// messageHandler is the function that processes incoming messages.
	messageHandler MessageHandlerFunc
	// exceptionHandler is the function that handles exceptions during message processing.
	exceptionHandler ExceptionHandlerFunc
	// shutdownHook is a function that will be called when the consumer is shutting down.
	shutdownHook func()
	// pollingConcurrency is the number of goroutines that will poll for messages.
	pollingConcurrency int
	// processingConcurrency is the number of goroutines that will process messages concurrently.
	processingConcurrency int
	// maxMessagesPerBatch is the maximum number of messages to receive in a single batch.
	maxMessagesPerBatch int32
	// waitTimeSeconds is the duration (in seconds) to wait for a message to arrive in the queue.
	waitTimeSeconds int32
	// pollIntervalMilliseconds is the interval between polling attempts in milliseconds.
	pollIntervalMilliseconds int32
}

// ApplyConsumerDefaults contains the default configuration for a new sqs consumer.
var ApplyConsumerDefaults = func(c *sqsConsumer) error {
	if c.logger.GetLevel() == zerolog.Disabled {
		c.logger = zerolog.Nop()
	}
	if c.client == nil {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return fmt.Errorf("unable to load AWS SDK config: %w", err)
		}
		c.client = sqs.NewFromConfig(cfg)
	}
	if c.pollingConcurrency == 0 {
		c.pollingConcurrency = DefaultPollingConcurrency
	}
	if c.processingConcurrency == 0 {
		c.processingConcurrency = DefaultProcessingConcurrency
	}
	if c.maxMessagesPerBatch == 0 {
		c.maxMessagesPerBatch = DefaultMaxMessagesPerBatch
	}
	if c.waitTimeSeconds == 0 {
		c.waitTimeSeconds = DefaultWaitTimeSeconds
	}
	if c.pollIntervalMilliseconds == 0 {
		c.pollIntervalMilliseconds = DefaultPollIntervalMilliseconds
	}
	if c.exceptionHandler == nil {
		c.exceptionHandler = func(ctx context.Context, err error) {
			c.logger.Error().Err(err).Msg("An exception occurred during message processing")
		}
	}
	return nil
}

func New(queueURL string, options ...Option) (Consumer, error) {
	if queueURL == "" {
		return nil, fmt.Errorf("queueURL cannot be empty")
	}
	c := &sqsConsumer{
		queueURL: queueURL,
	}
	for _, opt := range options {
		opt(c)
	}
	if c.messageHandler == nil {
		return nil, fmt.Errorf("messageHandler cannot be nil")
	}
	if err := ApplyConsumerDefaults(c); err != nil {
		return nil, err
	}
	c.logger = c.logger.With().Str("service", "consumer").Str("queueURL", c.queueURL).Logger()
	return c, nil
}

func (c *sqsConsumer) Start(ctx context.Context) error {
	c.logger.Info().
		Int("pollingGoroutines", c.pollingConcurrency).
		Int("processingConcurrency", c.processingConcurrency).
		Msg("Starting consumer...")

	gCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	processGroup, processCtx := errgroup.WithContext(gCtx)
	processGroup.SetLimit(c.processingConcurrency)

	var pollerWg sync.WaitGroup
	var pollErr atomic.Value
	for i := 0; i < c.pollingConcurrency; i++ {
		pollerWg.Add(1)
		pollerID := i
		go func() {
			defer pollerWg.Done()
			if err := c.poll(processCtx, pollerID, processGroup); err != nil {
				if !errors.Is(err, context.Canceled) {
					c.logger.Error().Err(err).Msg("Fatal poller error, initiating shutdown.")
					pollErr.Store(err)
					cancel()
				}
			}
		}()
	}
	// Wait for the context to be canceled, which can happen due to a parent context cancellation, an error from processing goroutines, or a fatal error from polling goroutines.
	<-processCtx.Done()
	// Wait for all polling goroutines to finish.
	pollerWg.Wait()
	// Wait for all processing goroutines to finish.
	c.logger.Info().Msg("Consumer shutting down...")
	if c.shutdownHook != nil {
		c.shutdownHook()
	}
	c.logger.Info().Msg("Consumer stopped.")
	if v := pollErr.Load(); v != nil {
		return v.(error)
	}
	if err := processGroup.Wait(); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (c *sqsConsumer) poll(ctx context.Context, pollerID int, processGroup *errgroup.Group) error {
	log := c.logger.With().Int("pollerID", pollerID).Logger()
	log.Info().Msg("Polling loop started.")

	for {
		select {
		case <-ctx.Done():
			log.Info().Err(ctx.Err()).Msg("Polling loop stopping due to context cancellation.")
			return ctx.Err()
		default:
			if err := c.receiveAndProcessMessages(ctx, log, processGroup); err != nil {
				var qne *types.QueueDoesNotExist
				c.exceptionHandler(ctx, err)
				if errors.As(err, &qne) {
					log.Error().Err(err).Msg("Queue does not exist.")
					return qne
				}
				log.Error().Err(err).Msg("An error occurred during message reception, continuing...")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(c.pollIntervalMilliseconds) * time.Millisecond):
			}
		}
	}
}

func (c *sqsConsumer) receiveAndProcessMessages(ctx context.Context, log zerolog.Logger, processGroup *errgroup.Group) error {
	req := &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(c.queueURL),
		MaxNumberOfMessages:         c.maxMessagesPerBatch,
		WaitTimeSeconds:             c.waitTimeSeconds,
		MessageAttributeNames:       []string{string(types.MessageSystemAttributeNameAll)},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	}

	resp, err := c.client.ReceiveMessage(ctx, req)
	if err != nil {
		return err
	}
	if len(resp.Messages) > 0 {
		log.Debug().Int("count", len(resp.Messages)).Msg("Received messages")
		for i := range resp.Messages {
			msg := resp.Messages[i]
			// SQS processors fits perfectly into the errgroup model. They are typical “subtasks” — numerous, short-lived,
			// and potentially error-prone — for which we need to limit concurrency and capture errors.
			processGroup.Go(func() error {
				c.handleMessage(ctx, &msg)
				// Ignore all errors from processing messages, as they are handled in handleMessage.
				// Maybe we can improve this later by returning an error from handleMessage
				return nil
			})
		}
	}
	return nil
}

// handleMessage processes a single SQS message.
func (c *sqsConsumer) handleMessage(ctx context.Context, msg *types.Message) {
	msgLog := c.logger.With().Str("messageID", *msg.MessageId).Logger()
	msgLog.Debug().Msg("Processing message")

	processingErr := c.messageHandler(ctx, msg)
	if processingErr == nil {
		msgLog.Debug().Msg("Message processed successfully, deleting.")
		if err := c.deleteMessage(ctx, msg.ReceiptHandle); err != nil {
			msgLog.Error().Err(err).Msg("Failed to delete message after successful processing.")
			c.exceptionHandler(ctx, err)
		}
		return
	}

	if errors.Is(processingErr, ErrNonRetryable) {
		msgLog.Warn().Err(processingErr).Msg("Non-retryable error, deleting message.")
		if err := c.deleteMessage(ctx, msg.ReceiptHandle); err != nil {
			msgLog.Error().Err(err).Msg("Failed to delete message after non-retryable error.")
			c.exceptionHandler(ctx, err)
		}
		return
	}

	msgLog.Error().Err(processingErr).Msg("Retryable error, changing message visibility to 0 for immediate retry.")
	if err := c.changeMessageVisibility(ctx, msg.ReceiptHandle, 0); err != nil {
		msgLog.Error().Err(err).Msg("Failed to NACK message after processing error.")
		c.exceptionHandler(ctx, err)
	}
}

func (c *sqsConsumer) deleteMessage(ctx context.Context, receiptHandle *string) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: receiptHandle,
	})
	return err
}

func (c *sqsConsumer) changeMessageVisibility(ctx context.Context, receiptHandle *string, timeout int32) error {
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.queueURL),
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: timeout,
	})
	return err
}
