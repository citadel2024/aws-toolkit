package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	. "github.com/citadel2024/aws-toolkit/sqs"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNew_Gomock(t *testing.T) {
	dummyHandler := func(ctx context.Context, msg *types.Message) error { return nil }

	t.Run("Success", func(t *testing.T) {
		c, err := New("test-queue", WithMessageHandler(dummyHandler))
		assert.NoError(t, err)
		assert.NotNil(t, c)
	})

	t.Run("Error on empty queue URL", func(t *testing.T) {
		_, err := New("", WithMessageHandler(dummyHandler))
		assert.Error(t, err)
	})

	t.Run("Error on nil message handler", func(t *testing.T) {
		_, err := New("test-queue")
		assert.Error(t, err)
	})
}

func TestConsumer_Start_Gomock(t *testing.T) {
	queueURL := "test-queue"
	messageID := "test-msg-id"
	receiptHandle := "test-receipt"
	msg := &types.Message{
		MessageId:     aws.String(messageID),
		ReceiptHandle: aws.String(receiptHandle),
		Body:          aws.String("hello world"),
	}

	setupConsumer := func(t *testing.T, handler MessageHandlerFunc) (*sqsConsumer, *MockClient) {
		ctrl := gomock.NewController(t)
		mockClient := NewMockClient(ctrl)

		c, err := New(
			queueURL,
			WithMessageHandler(handler),
			WithPollingGoroutines(1),
			WithProcessingConcurrency(1),
		)
		assert.NoError(t, err)

		consumer := c.(*sqsConsumer)
		consumer.client = mockClient
		consumer.pollIntervalMilliseconds = 1 // speed up testing
		return consumer, mockClient
	}

	t.Run("Process message successfully", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)

		handler := func(ctx context.Context, m *types.Message) error {
			assert.Equal(t, *msg.MessageId, *m.MessageId)
			return nil
		}

		c, mockClient := setupConsumer(t, handler)
		deleteInput := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: aws.String(receiptHandle),
		}

		mockClient.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any()).Return(&sqs.ReceiveMessageOutput{Messages: []types.Message{*msg}}, nil).AnyTimes()
		mockClient.EXPECT().DeleteMessage(gomock.Any(), deleteInput).Return(&sqs.DeleteMessageOutput{}, nil).Times(1).Do(func(ctx context.Context, input *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) {
			wg.Done()
		}).AnyTimes()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			wg.Wait()
			cancel()
		}()

		err := c.Start(ctx)
		assert.NoError(t, err)
	})

	t.Run("Handle non-retryable error", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)

		handler := func(ctx context.Context, m *types.Message) error {
			return ErrNonRetryable
		}

		c, mockClient := setupConsumer(t, handler)
		deleteInput := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: aws.String(receiptHandle),
		}

		mockClient.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any()).Return(&sqs.ReceiveMessageOutput{Messages: []types.Message{*msg}}, nil).AnyTimes()
		mockClient.EXPECT().DeleteMessage(gomock.Any(), deleteInput).Return(&sqs.DeleteMessageOutput{}, nil).Times(1).Do(func(ctx context.Context, input *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) {
			wg.Done()
		}).AnyTimes()
		mockClient.EXPECT().ChangeMessageVisibility(gomock.Any(), gomock.Any()).Times(0)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			wg.Wait()
			cancel()
		}()

		err := c.Start(ctx)
		assert.NoError(t, err)
	})

	t.Run("Handle retryable error", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)

		handler := func(ctx context.Context, m *types.Message) error {
			return errors.New("something went wrong")
		}
		c, mockClient := setupConsumer(t, handler)
		changeVisibilityInput := &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(queueURL),
			ReceiptHandle:     aws.String(receiptHandle),
			VisibilityTimeout: 0,
		}

		mockClient.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any()).Return(&sqs.ReceiveMessageOutput{Messages: []types.Message{*msg}}, nil).AnyTimes()
		mockClient.EXPECT().ChangeMessageVisibility(gomock.Any(), changeVisibilityInput).Return(&sqs.ChangeMessageVisibilityOutput{}, nil).Times(1).Do(func(ctx context.Context, input *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) {
			wg.Done()
		}).AnyTimes()
		mockClient.EXPECT().DeleteMessage(gomock.Any(), gomock.Any()).Times(0)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			wg.Wait()
			cancel()
		}()

		err := c.Start(ctx)
		assert.NoError(t, err)
	})

	t.Run("Handle fatal error QueueDoesNotExist", func(t *testing.T) {
		handler := func(ctx context.Context, m *types.Message) error { return nil }
		c, mockClient := setupConsumer(t, handler)
		qdeErr := &types.QueueDoesNotExist{Message: aws.String("queue not found")}

		mockClient.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any()).Return(nil, qdeErr).AnyTimes()

		var startErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			startErr = c.Start(context.Background())
		}()

		wg.Wait()

		assert.Error(t, startErr)
		assert.True(t, errors.Is(startErr, qdeErr))
	})

	t.Run("Consumer shuts down gracefully on context cancel", func(t *testing.T) {
		var shutdownHookCalled bool
		hook := func() {
			shutdownHookCalled = true
		}

		handler := func(ctx context.Context, m *types.Message) error { return nil }
		c, mockClient := setupConsumer(t, handler)
		c.shutdownHook = hook

		mockClient.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any()).Return(&sqs.ReceiveMessageOutput{Messages: []types.Message{}}, nil).AnyTimes()

		ctx, cancel := context.WithCancel(context.Background())

		var startErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			startErr = c.Start(ctx)
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()
		wg.Wait()

		assert.NoError(t, startErr)
		assert.True(t, shutdownHookCalled)
	})
}
