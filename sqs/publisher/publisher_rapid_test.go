package publisher

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"pgregory.net/rapid"
)

// entryGenerator creates a generator for SQS SendMessageBatchRequestEntry.
// It ensures that generated IDs and message bodies are non-empty.
func entryGenerator() *rapid.Generator[types.SendMessageBatchRequestEntry] {
	return rapid.Custom(func(t *rapid.T) types.SendMessageBatchRequestEntry {
		return types.SendMessageBatchRequestEntry{
			Id:          aws.String(rapid.String().Filter(func(s string) bool { return s != "" }).Draw(t, "id")),
			MessageBody: aws.String(rapid.String().Filter(func(s string) bool { return s != "" }).Draw(t, "messageBody")),
		}
	})
}

// TestRapidPublisherBatchingProperties checks fundamental properties of the SQS publisher's batching mechanism.
func TestRapidPublisherBatchingProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a list of messages to be published.
		messages := rapid.SliceOf(entryGenerator()).Draw(t, "messages")
		// Generate a batch size limit between 1 and 10.
		batchSize := rapid.IntRange(1, 10).Draw(t, "batchSize")
		// Generate a publishing interval.
		publishInterval := time.Duration(rapid.IntRange(10, 50).Draw(t, "publishInterval")) * time.Millisecond

		mockClient := &mockSQSClient{}
		publisher := New(
			"test-queue-url",
			WithClient(mockClient),
			WithBatchMaxMessages(batchSize),
			WithPublishInterval(publishInterval),
		).(*sqsPublisher)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := publisher.Start(ctx)
		if err != nil {
			t.Fatalf("Failed to start publisher: %v", err)
		}

		var totalSent int
		for _, msg := range messages {
			// Try to publish the message. If the channel is full, we just stop for this test run.
			if err := publisher.PublishMessageBatchWithEntry(ctx, msg); err != nil {
				break
			}
			totalSent++
		}

		// Allow time for the publisher to process and send the final batch.
		time.Sleep(publishInterval * 2)

		// Shutdown the publisher to ensure all buffered messages are flushed.
		if err := publisher.Shutdown(ctx); err != nil {
			t.Fatalf("Failed to shutdown publisher: %v", err)
		}

		mockClient.mu.Lock()
		defer mockClient.mu.Unlock()

		// 1. All successfully published messages should be captured in the mock client.
		var totalInBatches int
		for _, batch := range mockClient.sendMessageBatchInputs {
			totalInBatches += len(batch.Entries)
		}
		if totalSent != totalInBatches {
			t.Fatalf("Mismatch in message counts: sent %d, but found %d in batches", totalSent, totalInBatches)
		}

		// 2. No batch should exceed the configured batch size limit.
		for i, batch := range mockClient.sendMessageBatchInputs {
			if len(batch.Entries) > batchSize {
				t.Errorf("Batch %d exceeded max size: got %d, want <= %d", i, len(batch.Entries), batchSize)
			}
		}

		// 3. The messages in the batches should match the original messages sent.
		var receivedMessages []types.SendMessageBatchRequestEntry
		for _, batch := range mockClient.sendMessageBatchInputs {
			receivedMessages = append(receivedMessages, batch.Entries...)
		}

		if len(receivedMessages) != totalSent {
			t.Fatalf("Number of received messages (%d) does not match total sent (%d)", len(receivedMessages), totalSent)
		}

		// Check if the content of sent messages matches received messages.
		// This is a simplified check; a more robust check would use maps to handle out-of-order delivery.
		for i := 0; i < totalSent; i++ {
			originalID := aws.ToString(messages[i].Id)
			receivedID := aws.ToString(receivedMessages[i].Id)
			if originalID != receivedID {
				t.Errorf("Message ID mismatch at index %d: got %s, want %s", i, receivedID, originalID)
			}
		}
	})
}
