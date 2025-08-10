package publisher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	. "github.com/citadel2024/aws-toolkit/sqs"
)

// FuzzPublishMessageBatchWithEntry performs fuzz testing on the PublishMessageBatchWithEntry method.
// It aims to uncover bugs or crashes by providing a wide range of random inputs for the message ID and body.
func FuzzPublishMessageBatchWithEntry(f *testing.F) {
	// Seed the fuzzer with some initial valid values.
	f.Add("test-id-1", "hello world")
	f.Add("another-id", "some other message body")
	f.Add("!@#$", "special chars body")

	// The fuzzing engine will now generate random inputs based on the seeds.
	f.Fuzz(func(t *testing.T, id string, messageBody string) {
		mockClient := &mockSQSClient{}
		publisher := New(
			"test-queue-url",
			WithClient(mockClient),
			WithBatchMaxMessages(10),
			WithPublishInterval(100*time.Millisecond),
		).(*sqsPublisher)

		// Start the publisher's background worker.
		err := publisher.Start(context.Background())
		if err != nil {
			t.Fatalf("Failed to start publisher: %v", err)
		}
		defer publisher.Shutdown(context.Background())

		// Create the entry to be published.
		entry := types.SendMessageBatchRequestEntry{
			Id:          aws.String(id),
			MessageBody: aws.String(messageBody),
		}

		// Call the method under test.
		err = publisher.PublishMessageBatchWithEntry(context.Background(), entry)

		// We expect specific errors for empty ID or body, otherwise no error should occur.
		if messageBody == "" {
			if !errors.Is(err, ErrMessageBodyEmpty) {
				t.Errorf("Expected ErrMessageBodyEmpty for empty message body, but got: %v", err)
			}
			if id == "" {
				return
			}
		}
		if id == "" && !errors.Is(err, ErrMessageIDEmpty) {
			t.Errorf("Expected ErrMessageIDEmpty for empty ID, but got: %v", err)
		}

		// For valid inputs, we don't expect an error from the publish call itself.
		// The actual sending is asynchronous.
		if id != "" && messageBody != "" && err != nil && !errors.Is(err, ErrMessageChannelFull) {
			t.Errorf("PublishMessageBatchWithEntry returned an unexpected error for valid input: %v", err)
		}
	})
}
