package sqsx

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Client interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)

	SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)

	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)

	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)

	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

type Publisher interface {
	Start(ctx context.Context) error

	Shutdown(ctx context.Context) error

	PublishMessage(ctx context.Context, messageBody string) error

	PublishMessageWithInput(ctx context.Context, input *sqs.SendMessageInput) error

	PublishMessageBatch(ctx context.Context, id, messageBody string) error

	PublishMessageBatchWithEntry(ctx context.Context, entry types.SendMessageBatchRequestEntry) error

	IsStarted() bool

	IsClosed() bool

	GetQueueURL() string
}

type Consumer interface {
	Start(ctx context.Context) error
}
