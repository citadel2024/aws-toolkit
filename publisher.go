package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/rs/zerolog"
)

type sqsPublisher struct {
	// queueURL is the SQS queue URL where messages will be sent.
	queueURL string
	// logger is used for logging events and errors. Default zerolog.Nop()
	logger zerolog.Logger
	// client is the SQS client used to send messages.
	client SQSClient
	// batchMessagesLimit is the maximum number of messages per batch.
	// The user should estimate the number of batches based on the distribution of message sizes.
	batchMessagesLimit int
	// publishInterval defines the duration to wait before sending a batch of messages,
	// even if the batch size threshold has not been reached.
	publishInterval time.Duration
	// sendingMessageTimeoutSeconds is the timeout for sending a message batch.
	sendingMessageTimeoutSeconds int
	// messageCh is a buffered channel for incoming messages.
	messagesCh chan types.SendMessageBatchRequestEntry
	// shutdown is a channel used to signal the publisher to stop processing messages.
	shutdown chan struct{}
	// onSendMessageComplete is called when a batch send operation completes.
	onSendMessageComplete func(*sqs.SendMessageBatchOutput, error)
	// done is closed when the message batching goroutine exits.
	done chan struct{}
	// Add synchronization for thread-safe operations, mu is used to protect `started` and `closed`
	mu sync.RWMutex
	// started tracks whether the message batching goroutine has been launched.
	// Prevents multiple goroutines from being started and ensures messages
	// are only published after the consumer is ready.
	started bool
	// closed indicates the publisher has been shut down and should not
	// accept new messages or be restarted. Prevents use after shutdown
	// and duplicate shutdown operations.
	closed bool
}

type SQSPublisherOpt func(*sqsPublisher)

func WithLogger(logger zerolog.Logger) SQSPublisherOpt {
	return func(p *sqsPublisher) {
		p.logger = logger
	}
}

func WithClient(client SQSClient) SQSPublisherOpt {
	return func(p *sqsPublisher) {
		p.client = client
	}
}

func WithPublishInterval(interval time.Duration) SQSPublisherOpt {
	return func(p *sqsPublisher) {
		p.publishInterval = interval
	}
}

func WithBatchMaxMessages(limit int) SQSPublisherOpt {
	return func(p *sqsPublisher) {
		p.batchMessagesLimit = min(limit, SQSBatchMessagesLimit)
	}
}

func WithSendingMessageTimeoutSeconds(timeout int) SQSPublisherOpt {
	return func(p *sqsPublisher) {
		p.sendingMessageTimeoutSeconds = timeout
	}
}

func WithOnSendMessageComplete(handler func(*sqs.SendMessageBatchOutput, error)) SQSPublisherOpt {
	return func(p *sqsPublisher) {
		p.onSendMessageComplete = handler
	}
}

// ApplyPublisherDefaults contains the default configuration for a new sqsPublisher.
var ApplyPublisherDefaults = func(p *sqsPublisher) {
	if p.logger.GetLevel() == zerolog.Disabled {
		p.logger = zerolog.Nop()
	}
	if p.client == nil {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			panic(fmt.Sprintf("unable to load AWS SDK config, %v", err))
		}
		p.client = sqs.NewFromConfig(cfg)
	}
	if p.batchMessagesLimit == 0 {
		p.batchMessagesLimit = DefaultMaxNumberOfMessages
	}
	if p.publishInterval == 0 {
		p.publishInterval = DefaultPublishInterval
	}
	if p.sendingMessageTimeoutSeconds == 0 {
		p.sendingMessageTimeoutSeconds = DefaultSendingMessageTimeoutSeconds
	}
	if p.onSendMessageComplete == nil {
		p.onSendMessageComplete = func(out *sqs.SendMessageBatchOutput, err error) {
			if err != nil {
				p.logger.Error().Err(err).Msg("unable to send message batch")
			}
			if out != nil && len(out.Failed) > 0 {
				for _, entry := range out.Failed {
					p.logger.Error().
						Str("entryId", aws.ToString(entry.Id)).
						Str("entryCode", aws.ToString(entry.Code)).
						Str("entryMessage", aws.ToString(entry.Message)).
						Msg("failed to send message")
				}
			}
		}
	}

	p.messagesCh = make(chan types.SendMessageBatchRequestEntry, p.batchMessagesLimit)
	p.shutdown = make(chan struct{})
	p.done = make(chan struct{})
}

// NewSQSPublisher returns a new sqsPublisher with sensible defaults.
func NewSQSPublisher(queueURL string, opts ...SQSPublisherOpt) SQSPublisher {
	if queueURL == "" {
		panic("queueURL cannot be empty")
	}
	p := &sqsPublisher{
		queueURL: queueURL,
	}
	for _, opt := range opts {
		opt(p)
	}
	ApplyPublisherDefaults(p)
	return p
}

// PublishMessageBatch adds entry to the messages buffer channel.
func (p *sqsPublisher) PublishMessageBatch(entry types.SendMessageBatchRequestEntry) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPublisherClosed
	}
	if !p.started {
		p.mu.RUnlock()
		return ErrPublisherNotStarted
	}
	p.mu.RUnlock()

	if entry.MessageBody == nil || aws.ToString(entry.MessageBody) == "" {
		return ErrMessageBodyEmpty
	}
	if entry.Id == nil || aws.ToString(entry.Id) == "" {
		return ErrMessageIDEmpty
	}
	select {
	// messagesCh is thread-safe
	case p.messagesCh <- entry:
		return nil
	case <-p.shutdown:
		return ErrPublisherShutdown
	default:
		p.logger.Warn().Msg("Message channel is full, message rejected.")
		return ErrMessageChannelFull
	}
}

// Start changes the `started` value and starts the message batch sending worker.
func (p *sqsPublisher) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return ErrPublisherAlreadyStarted
	}
	if p.closed {
		p.mu.Unlock()
		return ErrPublisherClosed
	}
	p.started = true
	p.mu.Unlock()

	go p.startSendMessageBatchWorker()
	return nil
}

// Shutdown shuts the sqsPublisher message batching routine down cleanly.
func (p *sqsPublisher) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil // Already closed
	}
	if !p.started {
		p.closed = true
		p.mu.Unlock()
		return nil // Not started, just mark as closed
	}
	p.closed = true
	p.mu.Unlock()

	close(p.shutdown)
	// Wait for the message batching goroutine to finish or context to be done
	// This allows the publisher to finish processing any remaining messages,
	// usually we advise the user to use a longer context timeout
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return nil
	}
}

func (p *sqsPublisher) startSendMessageBatchWorker() {
	defer close(p.done)
	input := &sqs.SendMessageBatchInput{
		QueueUrl: &p.queueURL,
		Entries:  make([]types.SendMessageBatchRequestEntry, 0, p.batchMessagesLimit),
	}
	var lastBatchSend time.Time
	appendToBatch := func(entry types.SendMessageBatchRequestEntry) {
		p.logger.Debug().
			Str("entryId", aws.ToString(entry.Id)).
			Str("entryMessageBody", aws.ToString(entry.MessageBody)).
			Msg("Appending message to batch")
		input.Entries = append(input.Entries, entry)
		if len(input.Entries) >= p.batchMessagesLimit {
			p.sendMessageBatch(input)
			lastBatchSend = time.Now()
		}
	}
	sendBatchIfStale := func(tick time.Time) {
		if len(input.Entries) > 0 && tick.Sub(lastBatchSend) >= p.publishInterval {
			p.logger.Debug().
				Int("entryCount", len(input.Entries)).
				Msg("Sending batch due to publish interval")
			p.sendMessageBatch(input)
			lastBatchSend = tick
		}
	}

	ticker := time.NewTicker(p.publishInterval)
	defer ticker.Stop()

	// Main event loop that handles batching and shutdown logic.
	// - On ticker tick: attempts to send the current batch if it's stale.
	// - On new message: appends the message to the current batch.
	// - On shutdown signal: breaks the loop and proceeds to drain remaining messages.
	for {
		select {
		case tick := <-ticker.C:
			sendBatchIfStale(tick)
		case m := <-p.messagesCh:
			appendToBatch(m)
		case <-p.shutdown:
			p.logger.Info().Msg("sqsPublisher shutting down, draining message queue")
			for entry := range p.messagesCh {
				appendToBatch(entry)
			}
			if len(input.Entries) > 0 {
				p.logger.Info().Int("remainingEntries", len(input.Entries)).Msg("Sending final message batch")
				p.sendMessageBatch(input)
			}
			p.logger.Info().Msg("sqsPublisher shutdown cleanly")
			return
		}
	}
}

func (p *sqsPublisher) sendMessageBatch(input *sqs.SendMessageBatchInput) {
	if len(input.Entries) == 0 {
		return
	}
	messageCount := len(input.Entries)
	p.logger.Info().Int("messageCount", messageCount).Msg("Sending message batch to SQS")

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.sendingMessageTimeoutSeconds)*time.Second)
	defer cancel()

	start := time.Now()
	out, err := p.client.SendMessageBatch(ctx, input)
	duration := time.Since(start)
	loggerWithCtx := p.logger.With().Int("messageCount", messageCount).Dur("duration", duration).Logger()
	if err != nil {
		failedCount := 0
		if out != nil {
			failedCount = len(out.Failed)
		}
		loggerWithCtx.Error().Err(err).Int("failedCount", failedCount).Msg("Failed to send message batch")
	} else {
		loggerWithCtx.Info().Msg("Send message batch completed")
	}
	p.onSendMessageComplete(out, err)
	// Clear entries for next batch (reuse slice to avoid allocations)
	input.Entries = input.Entries[:0]
}

func (p *sqsPublisher) IsStarted() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.started
}

func (p *sqsPublisher) IsClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

func (p *sqsPublisher) GetQueueURL() string {
	return p.queueURL
}
