package cloudwatchx

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

type CloudwatchPutMetricDataAPI interface {
	PutMetricData(ctx context.Context, params *cloudwatch.PutMetricDataInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error)
}
