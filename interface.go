package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSClient interface {
	SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

type SQSPublisher interface {
	Start(ctx context.Context) error

	Shutdown(ctx context.Context) error

	PublishMessageBatch(entry types.SendMessageBatchRequestEntry) error

	IsStarted() bool

	IsClosed() bool

	GetQueueURL() string
}
