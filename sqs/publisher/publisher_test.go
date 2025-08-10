package publisher

import (
	"context"
	"fmt"
	"github.com/jinzhu/copier"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/citadel2024/aws-toolkit/sqs"
)

// mockSQSClient is a mock implementation of the SQS client for testing purposes.
type mockSQSClient struct {
	mu                     sync.Mutex
	sendMessageBatchInputs []*sqs.SendMessageBatchInput
	sendMessageInputs      []*sqs.SendMessageInput
	sendMessageBatchErr    error
	sendMessageErr         error
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	panic("implement me")
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	panic("implement me")
}

func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	panic("implement me")
}

func newMockSQSClient() *mockSQSClient {
	return &mockSQSClient{
		sendMessageBatchInputs: make([]*sqs.SendMessageBatchInput, 0),
		sendMessageInputs:      make([]*sqs.SendMessageInput, 0),
	}
}

func (m *mockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendMessageInputs = append(m.sendMessageInputs, params)
	if m.sendMessageErr != nil {
		return nil, m.sendMessageErr
	}
	return &sqs.SendMessageOutput{MessageId: aws.String("mock-message-id")}, nil
}

func (m *mockSQSClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var dst sqs.SendMessageBatchInput
	_ = copier.Copy(&dst, params)
	m.sendMessageBatchInputs = append(m.sendMessageBatchInputs, &dst)
	if m.sendMessageBatchErr != nil {
		return &sqs.SendMessageBatchOutput{
			Failed: []types.BatchResultErrorEntry{{Id: aws.String("failed-id")}},
		}, m.sendMessageBatchErr
	}
	return &sqs.SendMessageBatchOutput{
		Successful: []types.SendMessageBatchResultEntry{{Id: aws.String("success-id")}},
	}, nil
}

func (m *mockSQSClient) getBatchCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sendMessageBatchInputs)
}

func (m *mockSQSClient) getAllBatchEntries() []types.SendMessageBatchRequestEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	var allEntries []types.SendMessageBatchRequestEntry
	for _, input := range m.sendMessageBatchInputs {
		allEntries = append(allEntries, input.Entries...)
	}
	return allEntries
}

func (m *mockSQSClient) getSingleSendCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sendMessageInputs)
}

func TestNew(t *testing.T) {
	assert.Panics(t, func() {
		New("")
	}, "New should panic with empty queueURL")

	mockClient := newMockSQSClient()
	p := New("test-queue", WithClient(mockClient), WithBatchMaxMessages(5))
	require.NotNil(t, p)

	publisher := p.(*sqsPublisher)
	assert.Equal(t, "test-queue", publisher.queueURL)
	assert.Equal(t, 5, publisher.batchMessagesLimit)
	assert.Equal(t, DefaultPublishInterval, publisher.publishInterval)
	assert.NotNil(t, publisher.client)
}

func TestPublisher_StatusAndErrors(t *testing.T) {
	mockClient := newMockSQSClient()
	p := New("test-queue", WithClient(mockClient))

	err := p.PublishMessageBatch(context.Background(), "id1", "body1")
	assert.Equal(t, ErrPublisherNotStarted, err)

	err = p.Start(context.Background())
	assert.NoError(t, err)
	assert.True(t, p.(*sqsPublisher).IsStarted())

	err = p.Start(context.Background())
	assert.Equal(t, ErrPublisherAlreadyStarted, err)

	err = p.Shutdown(context.Background())
	assert.NoError(t, err)
	assert.True(t, p.(*sqsPublisher).IsClosed())

	err = p.PublishMessageBatch(context.Background(), "id2", "body2")
	assert.Equal(t, ErrPublisherClosed, err)

	err = p.Start(context.Background())
	assert.Equal(t, ErrPublisherClosed, err)
}

func TestPublisher_PublishMessageWithInput(t *testing.T) {
	mockClient := newMockSQSClient()
	p := New("test-queue", WithClient(mockClient))

	err := p.PublishMessageWithInput(context.Background(), &sqs.SendMessageInput{
		MessageBody: aws.String("hello"),
		QueueUrl:    aws.String("test-queue"),
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, mockClient.getSingleSendCallCount())
	assert.Equal(t, "hello", *mockClient.sendMessageInputs[0].MessageBody)
}

func TestPublisher_BatchingBySize(t *testing.T) {
	mockClient := newMockSQSClient()
	p := New("test-queue",
		WithClient(mockClient),
		WithBatchMaxMessages(5),
		WithPublishInterval(5*time.Millisecond),
	)

	err := p.Start(context.Background())
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		err := p.PublishMessageBatch(context.Background(), fmt.Sprintf("id-%d", i), "body")
		assert.NoError(t, err)
	}

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, mockClient.getBatchCallCount(), "should have sent one batch")
	allEntries := mockClient.getAllBatchEntries()
	assert.Len(t, allEntries, 5)

	err = p.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestPublisher_BatchingByInterval(t *testing.T) {
	mockClient := newMockSQSClient()
	p := New("test-queue",
		WithClient(mockClient),
		WithBatchMaxMessages(10),
		WithPublishInterval(50*time.Millisecond),
	)

	err := p.Start(context.Background())
	require.NoError(t, err)

	err = p.PublishMessageBatch(context.Background(), "id-1", "body")
	assert.NoError(t, err)
	assert.Equal(t, 0, mockClient.getBatchCallCount(), "should not send batch immediately")

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, mockClient.getBatchCallCount(), "should send batch after interval")
	allEntries := mockClient.getAllBatchEntries()
	assert.Len(t, allEntries, 1)

	err = p.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestPublisher_ShutdownDrain(t *testing.T) {
	mockClient := newMockSQSClient()
	p := New("test-queue",
		WithClient(mockClient),
		WithBatchMaxMessages(10),
		WithPublishInterval(5*time.Second),
	)

	err := p.Start(context.Background())
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err := p.PublishMessageBatch(context.Background(), fmt.Sprintf("id-%d", i), "body")
		assert.NoError(t, err)
	}

	assert.Equal(t, 0, mockClient.getBatchCallCount(), "should not have sent any batch yet")

	err = p.Shutdown(context.Background())
	assert.NoError(t, err)

	assert.Equal(t, 1, mockClient.getBatchCallCount(), "shutdown should trigger a final batch send")
	allEntries := mockClient.getAllBatchEntries()
	assert.Len(t, allEntries, 3, "final batch should contain all remaining messages")
}

func TestPublisher_ConcurrentPublish(t *testing.T) {
	mockClient := newMockSQSClient()
	p := New("test-queue",
		WithClient(mockClient),
		WithBatchMaxMessages(10),
		WithPublishInterval(100*time.Millisecond),
	)

	err := p.Start(context.Background())
	require.NoError(t, err)

	var wg sync.WaitGroup
	publishCount := 10
	wg.Add(publishCount)

	for i := 0; i < publishCount; i++ {
		go func(id int) {
			defer wg.Done()
			err := p.PublishMessageBatch(context.Background(), fmt.Sprintf("id-%d", id), "body")
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
	err = p.Shutdown(context.Background())
	assert.NoError(t, err)

	allEntries := mockClient.getAllBatchEntries()
	assert.Len(t, allEntries, publishCount, "all concurrently published messages should be sent")
}

func TestPublisher_MessageChannelFull(t *testing.T) {
	mockClient := newMockSQSClient()
	// Set a small batch size and a long interval to easily fill the channel
	p := New("test-queue",
		WithClient(mockClient),
		WithBatchMaxMessages(1),
		WithPublishInterval(10*time.Second),
	)

	err := p.Start(context.Background())
	require.NoError(t, err)

	// Fill the channel (size is batchMessagesLimit)
	err = p.PublishMessageBatch(context.Background(), "id-1", "body")
	require.NoError(t, err)

	// This message should be rejected
	err = p.PublishMessageBatch(context.Background(), "id-2", "body")
	assert.Equal(t, ErrMessageChannelFull, err)

	err = p.Shutdown(context.Background())
	assert.NoError(t, err)
}
