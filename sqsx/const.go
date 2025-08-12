package sqsx

import "time"

const (
	DefaultPublishInterval = 1 * time.Second

	DefaultSendingMessageTimeoutSeconds = 30
)

const (
	DefaultBatchMessagesLimit = 10

	// DefaultMaxNumberOfMessages defaults to the maximum number of messages the
	// Server can request when receiving messages.
	DefaultMaxNumberOfMessages = 10
)

const (
	// DefaultProcessingConcurrency is the default concurrency for processing messages in sqs consumer.
	DefaultProcessingConcurrency = 10

	// DefaultPollingConcurrency is the default concurrency for polling messages in sqs consumer.
	DefaultPollingConcurrency = 1

	// DefaultMaxMessagesPerBatch is the default maximum number of messages per batch
	DefaultMaxMessagesPerBatch = 10

	// DefaultWaitTimeSeconds is the default wait time in milliseconds for long polling
	DefaultWaitTimeSeconds = 20

	// DefaultPollIntervalMilliseconds is the default interval in milliseconds between polling attempts
	DefaultPollIntervalMilliseconds = 0
)
