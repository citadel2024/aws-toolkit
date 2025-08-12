package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	. "github.com/citadel2024/aws-toolkit/sqsx"
	"github.com/rs/zerolog"
	"pgregory.net/rapid"
	"sync"
	"sync/atomic"
	"testing"
)

type mockSQSClient struct {
	mu           sync.Mutex
	messages     []types.Message
	received     map[string]bool
	deleted      map[string]bool
	nacked       map[string]bool
	receiveErr   error
	deleteErr    error
	changeVisErr error
}

func (m *mockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	panic("implement me")
}

func (m *mockSQSClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	panic("implement me")
}

func newMockSQSClient(messages []types.Message) *mockSQSClient {
	return &mockSQSClient{
		messages: messages,
		received: make(map[string]bool),
		deleted:  make(map[string]bool),
		nacked:   make(map[string]bool),
	}
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if m.receiveErr != nil {
		return nil, m.receiveErr
	}

	m.mu.Lock()
	if len(m.messages) > 0 {
		batchSize := int(params.MaxNumberOfMessages)
		if batchSize > len(m.messages) {
			batchSize = len(m.messages)
		}

		batch := m.messages[:batchSize]
		m.messages = m.messages[batchSize:]
		m.mu.Unlock()
		return &sqs.ReceiveMessageOutput{Messages: batch}, nil
	}
	m.mu.Unlock()
	<-ctx.Done()
	return &sqs.ReceiveMessageOutput{Messages: []types.Message{}}, ctx.Err()
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	m.deleted[*params.ReceiptHandle] = true
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.changeVisErr != nil {
		return nil, m.changeVisErr
	}
	m.nacked[*params.ReceiptHandle] = true
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func TestRapidConsumer_Start_Property(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		type messageOutcome int
		const (
			Success messageOutcome = iota
			RetryableError
			NonRetryableError
		)
		var outcomes = []messageOutcome{Success, RetryableError, NonRetryableError}
		var idCounter int64
		nextID := func() int64 {
			return atomic.AddInt64(&idCounter, 1)
		}
		messagesGen := rapid.SliceOf(rapid.Custom(func(t *rapid.T) struct {
			ID, Body string
			Outcome  messageOutcome
		} {
			return struct {
				ID, Body string
				Outcome  messageOutcome
			}{
				ID:      fmt.Sprintf("msg-%d-%d", nextID(), rapid.Int().Draw(t, "idSuffix")),
				Body:    rapid.String().Draw(t, "body"),
				Outcome: rapid.SampledFrom(outcomes).Draw(t, "outcome"),
			}
		}))

		configGen := rapid.Custom(func(t *rapid.T) struct {
			p, c int
			m    int32
		} {
			return struct {
				p, c int
				m    int32
			}{
				p: rapid.IntRange(1, 4).Draw(t, "polling"),
				c: rapid.IntRange(1, 10).Draw(t, "processing"),
				m: rapid.Int32Range(1, 10).Draw(t, "maxMessages"),
			}
		})
		generatedMessages := messagesGen.Draw(t, "messages")
		config := configGen.Draw(t, "config")

		var successful, nonRetryable, retryable atomic.Int32
		outcomeMap := sync.Map{}
		sqsMessages := make([]types.Message, len(generatedMessages))

		var processingWg sync.WaitGroup
		processingWg.Add(len(generatedMessages))

		for i, msgInfo := range generatedMessages {
			msgID, receiptHandle := msgInfo.ID, "receipt-"+msgInfo.ID
			outcomeMap.Store(msgID, msgInfo.Outcome)
			sqsMessages[i] = types.Message{MessageId: &msgID, Body: &msgInfo.Body, ReceiptHandle: &receiptHandle}
		}

		mockClient := newMockSQSClient(sqsMessages)

		handler := func(ctx context.Context, msg *types.Message) error {
			defer processingWg.Done()
			mockClient.mu.Lock()
			mockClient.received[*msg.MessageId] = true
			mockClient.mu.Unlock()

			outcome, _ := outcomeMap.Load(*msg.MessageId)
			switch outcome {
			case Success:
				successful.Add(1)
				return nil
			case NonRetryableError:
				nonRetryable.Add(1)
				return ErrNonRetryable
			case RetryableError:
				retryable.Add(1)
				return errors.New("a retryable error")
			default:
				return nil
			}
		}

		consumer, err := New("test-queue",
			WithLogger(zerolog.Nop()),
			WithMessageHandler(handler),
			WithPollingGoroutines(config.p),
			WithProcessingConcurrency(config.c),
			WithMaxMessagesPerBatch(config.m),
			WithWaitTimeSeconds(1),
			func(c *sqsConsumer) {
				c.client = mockClient
				c.pollIntervalMilliseconds = 10
			},
		)
		if err != nil {
			t.Fatalf("Failed to create consumer: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		var consumerErr error
		var consumerWg sync.WaitGroup
		consumerWg.Add(1)

		go func() {
			defer consumerWg.Done()
			consumerErr = consumer.Start(ctx)
		}()
		// Wait all messages to be processed and cancel context.
		go func() {
			processingWg.Wait()
			cancel()
		}()
		consumerWg.Wait()

		if consumerErr != nil && !errors.Is(consumerErr, context.Canceled) {
			t.Fatalf("Consumer returned an unexpected error: %v", consumerErr)
		}
		var numSuccessful, numNonRetryable, numRetryable atomic.Int32

		outcomeMap.Range(func(key, value interface{}) bool {
			outcome := value.(messageOutcome)
			switch outcome {
			case Success:
				numSuccessful.Add(1)
			case NonRetryableError:
				numNonRetryable.Add(1)
			case RetryableError:
				numRetryable.Add(1)
			}
			return true
		})
		if successful.Load() != numSuccessful.Load() {
			t.Errorf("mismatch successful: want %d, got %d", numSuccessful.Load(), successful.Load())
		}
		if nonRetryable.Load() != numNonRetryable.Load() {
			t.Errorf("mismatch non-retryable: want %d, got %d", numNonRetryable.Load(), nonRetryable.Load())
		}
		if retryable.Load() != numRetryable.Load() {
			t.Errorf("mismatch retryable: want %d, got %d", numRetryable.Load(), retryable.Load())
		}
		mockClient.mu.Lock()
		defer mockClient.mu.Unlock()

		if int32(len(mockClient.deleted)) != numSuccessful.Load()+numNonRetryable.Load() {
			t.Errorf("mismatch deleted: want %d, got %d", numSuccessful.Load()+numNonRetryable.Load(), len(mockClient.deleted))
		}
		if int32(len(mockClient.nacked)) != numRetryable.Load() {
			t.Errorf("mismatch nacked: want %d, got %d", numRetryable.Load(), len(mockClient.nacked))
		}
	})
}
