package sqsx

import "errors"

var (
	// ErrNonRetryable is a sentinel error.
	// When a MessageHandler returns this error, the consumer will consider the message "poisoned" or unprocessable,
	// and it will be deleted from the queue (avoiding retries).
	// Usage: fmt.Errorf("%w: invalid message format", sqs.ErrNonRetryable)
	ErrNonRetryable = errors.New("non-retryable error")

	// SQS publisher errors

	ErrPublisherClosed         = errors.New("publisher is closed")
	ErrPublisherNotStarted     = errors.New("publisher not started")
	ErrPublisherShutdown       = errors.New("publisher is shutting down")
	ErrPublisherAlreadyStarted = errors.New("publisher already started")

	ErrMessageChannelFull = errors.New("message channel is full")

	ErrMessageBodyEmpty = errors.New("message body cannot be empty")
	ErrMessageIDEmpty   = errors.New("message ID cannot be empty")

	// SQS consumer errors
)
