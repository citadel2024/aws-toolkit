package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	sqstoolkit "github.com/citadel2024/aws-toolkit/sqs"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// MockSQSClient implements the Client interface for testing
type MockSQSClient struct {
	receiveMessageFunc          func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	deleteMessageFunc           func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	changeMessageVisibilityFunc func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

func (m *MockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	panic("implement me")
}

func (m *MockSQSClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	panic("implement me")
}

func (m *MockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if m.receiveMessageFunc != nil {
		return m.receiveMessageFunc(ctx, params, optFns...)
	}
	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *MockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if m.deleteMessageFunc != nil {
		return m.deleteMessageFunc(ctx, params, optFns...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *MockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if m.changeMessageVisibilityFunc != nil {
		return m.changeMessageVisibilityFunc(ctx, params, optFns...)
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// FuzzMessageHandler fuzzes message handling with various message contents and error conditions
func FuzzMessageHandler(f *testing.F) {
	// Seed with various message scenarios
	f.Add("test-message-body", "test-message-id", "test-receipt-handle", true, false)
	f.Add("", "", "", false, false)
	f.Add("very long message body with special characters: \n\r\t\x00\xff", "msg-123", "handle-456", false, true)
	f.Add("{\"json\": \"message\", \"number\": 123}", "json-msg", "json-handle", true, false)

	f.Fuzz(func(t *testing.T, body, messageID, receiptHandle string, shouldSucceed, shouldBeNonRetryable bool) {
		if messageID == "" {
			return
		}
		// Track operations for verification
		var deleteCount, visibilityChangeCount int
		var mu sync.Mutex

		mockClient := &MockSQSClient{
			deleteMessageFunc: func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
				mu.Lock()
				deleteCount++
				mu.Unlock()
				return &sqs.DeleteMessageOutput{}, nil
			},
			changeMessageVisibilityFunc: func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
				mu.Lock()
				visibilityChangeCount++
				mu.Unlock()
				return &sqs.ChangeMessageVisibilityOutput{}, nil
			},
		}

		// Create message handler that simulates different error conditions
		handler := func(ctx context.Context, msg *types.Message) error {
			if shouldSucceed {
				return nil
			}
			if shouldBeNonRetryable {
				return fmt.Errorf("non-retryable error: %w", sqstoolkit.ErrNonRetryable)
			}
			return errors.New("retryable error")
		}

		consumer := &sqsConsumer{
			queueURL:       "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			client:         mockClient,
			messageHandler: handler,
			logger:         zerolog.Nop(),
			exceptionHandler: func(ctx context.Context, err error) {
			},
		}

		msg := &types.Message{
			Body: &body,
		}
		if messageID != "" {
			msg.MessageId = &messageID
		}
		if receiptHandle != "" {
			msg.ReceiptHandle = &receiptHandle
		}
		consumer.handleMessage(context.Background(), msg)

		mu.Lock()
		defer mu.Unlock()

		if shouldSucceed {
			if deleteCount != 1 {
				t.Errorf("Expected 1 delete operation for successful processing, got %d", deleteCount)
			}
			if visibilityChangeCount != 0 {
				t.Errorf("Expected 0 visibility changes for successful processing, got %d", visibilityChangeCount)
			}
		} else if shouldBeNonRetryable {
			if deleteCount != 1 {
				t.Errorf("Expected 1 delete operation for non-retryable error, got %d", deleteCount)
			}
			if visibilityChangeCount != 0 {
				t.Errorf("Expected 0 visibility changes for non-retryable error, got %d", visibilityChangeCount)
			}
		} else {
			if deleteCount != 0 {
				t.Errorf("Expected 0 delete operations for retryable error, got %d", deleteCount)
			}
			if visibilityChangeCount != 1 {
				t.Errorf("Expected 1 visibility change for retryable error, got %d", visibilityChangeCount)
			}
		}
	})
}

// FuzzReceiveAndProcessMessages fuzzes the message receiving and processing logic
func FuzzReceiveAndProcessMessages(f *testing.F) {
	// Seed with different message batch scenarios
	f.Add(0, false, false)  // No messages
	f.Add(1, false, false)  // Single message
	f.Add(10, false, false) // Full batch
	f.Add(5, true, false)   // Messages with receive error
	f.Add(3, false, true)   // Messages with processing delays

	f.Fuzz(func(t *testing.T, messageCount int, simulateReceiveError, simulateSlowProcessing bool) {
		// Limit message count to reasonable bounds
		if messageCount < 0 {
			messageCount = 0
		}
		if messageCount > 10 {
			messageCount = 10
		}

		var processedMessages sync.Map
		mockClient := &MockSQSClient{
			receiveMessageFunc: func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				if simulateReceiveError {
					return nil, errors.New("simulated receive error")
				}

				messages := make([]types.Message, messageCount)
				for i := 0; i < messageCount; i++ {
					msgID := fmt.Sprintf("msg-%d", i)
					receiptHandle := fmt.Sprintf("handle-%d", i)
					body := fmt.Sprintf("body-%d", i)
					messages[i] = types.Message{
						MessageId:     &msgID,
						ReceiptHandle: &receiptHandle,
						Body:          &body,
					}
				}
				return &sqs.ReceiveMessageOutput{
					Messages: messages,
				}, nil
			},
			deleteMessageFunc: func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
				return &sqs.DeleteMessageOutput{}, nil
			},
		}

		handler := func(ctx context.Context, msg *types.Message) error {
			if simulateSlowProcessing {
				time.Sleep(time.Millisecond * 10)
			}
			if msg.MessageId != nil {
				processedMessages.Store(*msg.MessageId, true)
			}
			return nil
		}

		consumer := &sqsConsumer{
			queueURL:              "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			client:                mockClient,
			messageHandler:        handler,
			logger:                zerolog.Nop(),
			processingConcurrency: 5,
			exceptionHandler: func(ctx context.Context, err error) {
			},
		}

		ctx := context.Background()
		log := zerolog.Nop()

		processGroup, _ := errgroup.WithContext(context.Background())
		processGroup.SetLimit(consumer.processingConcurrency)

		err := consumer.receiveAndProcessMessages(ctx, log, processGroup)

		if simulateReceiveError {
			if err == nil {
				t.Error("Expected error from receiveAndProcessMessages, got nil")
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error from receiveAndProcessMessages: %v", err)
			}
			time.Sleep(time.Millisecond * 50)
			actualProcessed := 0
			processedMessages.Range(func(key, value interface{}) bool {
				actualProcessed++
				return true
			})

			if actualProcessed != messageCount {
				t.Errorf("Expected %d processed messages, got %d", messageCount, actualProcessed)
			}
		}
	})
}

// FuzzPollingLoop fuzzes the polling loop with various timing and error conditions
func FuzzPollingLoop(f *testing.F) {
	// Seed with different polling scenarios
	f.Add(int32(0), int32(100), false, false)   // No wait, short poll interval
	f.Add(int32(20), int32(1000), false, false) // Max wait, long poll interval
	f.Add(int32(10), int32(500), true, false)   // Medium settings with queue error
	f.Add(int32(5), int32(50), false, true)     // Quick settings with context cancellation

	f.Fuzz(func(t *testing.T, waitTimeSeconds, pollIntervalMs int32, simulateQueueError, quickCancel bool) {
		// Ensure reasonable bounds
		if waitTimeSeconds < 0 {
			waitTimeSeconds = 0
		}
		if waitTimeSeconds > 20 {
			waitTimeSeconds = 20
		}
		if pollIntervalMs < 0 {
			pollIntervalMs = 0
		}
		if pollIntervalMs > 5000 {
			pollIntervalMs = 5000
		}

		var receiveCount int32

		mockClient := &MockSQSClient{
			receiveMessageFunc: func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				receiveCount++

				if simulateQueueError && receiveCount > 2 {
					return nil, &types.QueueDoesNotExist{
						Message: aws.String("Queue does not exist"),
					}
				}

				return &sqs.ReceiveMessageOutput{
					Messages: []types.Message{}, // Empty response
				}, nil
			},
		}

		consumer := &sqsConsumer{
			queueURL:                 "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			client:                   mockClient,
			messageHandler:           func(ctx context.Context, msg *types.Message) error { return nil },
			logger:                   zerolog.Nop(),
			waitTimeSeconds:          waitTimeSeconds,
			pollIntervalMilliseconds: pollIntervalMs,
			exceptionHandler:         func(ctx context.Context, err error) {},
		}

		ctx, cancel := context.WithCancel(context.Background())
		if quickCancel {
			// Cancel context quickly to test cancellation handling
			go func() {
				time.Sleep(time.Millisecond * 50)
				cancel()
			}()
		} else {
			// Cancel after a reasonable time for testing
			go func() {
				time.Sleep(time.Millisecond * 200)
				cancel()
			}()
		}

		processGroup, _ := errgroup.WithContext(context.Background())
		processGroup.SetLimit(5)

		err := consumer.poll(ctx, 0, processGroup)

		// Verify expected behavior
		if simulateQueueError {
			var qne *types.QueueDoesNotExist
			if !errors.As(err, &qne) && !errors.Is(err, context.Canceled) {
				t.Errorf("Expected QueueDoesNotExist or context.Canceled error, got: %v", err)
			}
		} else {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Expected context.Canceled error, got: %v", err)
			}
		}
	})
}
