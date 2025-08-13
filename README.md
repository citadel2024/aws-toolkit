
[![test](https://github.com/citadel2024/aws-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/citadel2024/aws-toolkit/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/citadel2024/aws-toolkit/branch/master/graph/badge.svg?token=WgmwqR76eQ)](https://codecov.io/gh/citadel2024/aws-toolkit)
[![Go Report Card](https://goreportcard.com/badge/github.com/citadel2024/aws-toolkit)](https://goreportcard.com/report/github.com/citadel2024/aws-toolkit)

# AWS Toolkit
A high-performance, production-ready Go library for Amazon Web Services (AWS) that provides utilities for working with AWS services like SQS (Simple Queue Service). This toolkit is designed to simplify the development of AWS applications with features like structured logging, error handling, and concurrency management.

## Installation

```bash
go get github.com/citadel2024/aws-toolkit
```

## Quick Start
See examples folder.


## SQS
A high-performance, production-ready Go library for Amazon SQS (Simple Queue Service) that provides both publishing and consuming capabilities with advanced features like batching, graceful shutdown, error handling, and comprehensive observability.

Inspired by https://github.com/awslabs/amazon-sqs-java-temporary-queues-client.

## Features

### 🚀 Publisher
- **Batch Processing**: Automatic message batching for optimal throughput
- **Configurable Intervals**: Time-based and size-based batch triggers
- **Graceful Shutdown**: Clean shutdown with message draining
- **Error Handling**: Comprehensive error handling with custom callbacks
- **Thread Safety**: Safe for concurrent use across multiple goroutines
- **Observability**: Structured logging with zerolog integration

### 📨 Consumer
- **Concurrent Processing**: Multi-threaded message polling and processing
- **Error Classification**: Distinguishes between retryable and non-retryable errors
- **Long Polling**: Efficient long polling to reduce API calls
- **Graceful Shutdown**: Coordinated shutdown of all processing goroutines
- **Custom Error Handling**: Pluggable exception handlers


## Configuration Options

### Publisher Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithLogger(logger)` | Custom zerolog logger | `zerolog.Nop()` |
| `WithClient(client)` | Custom SQS client | AWS default config |
| `WithBatchMaxMessages(limit)` | Max messages per batch | 10 |
| `WithPublishInterval(interval)` | Batch publish interval | 5 seconds |
| `WithSendingMessageTimeoutSeconds(timeout)` | Send timeout | 30 seconds |
| `WithOnSendMessageBatchComplete(handler)` | Batch completion callback | Default error logging |

### Consumer Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithLogger(logger)` | Custom zerolog logger | `zerolog.Nop()` |
| `WithMessageHandler(handler)` | Message processing function | **Required** |
| `WithExceptionHandler(handler)` | Exception handling function | Default error logging |
| `WithPollingGoroutines(count)` | Number of polling goroutines | 1 |
| `WithProcessingConcurrency(count)` | Max concurrent message processors | 10 |
| `WithMaxMessagesPerBatch(count)` | Messages per receive call (1-10) | 10 |
| `WithWaitTimeSeconds(seconds)` | Long polling wait time (0-20) | 20 |
| `WithShutdownHook(hook)` | Function called on shutdown | None |

## Advanced Usage

## Architecture

### Publisher Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────┐
│   Application   │───▶│   Publisher  │───▶│  SQS Queue  │
└─────────────────┘    └──────────────┘    └─────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Batch Worker    │
                    │  • Time-based    │
                    │  • Size-based    │
                    │  • Error handling│
                    └──────────────────┘
```

### Consumer Architecture

```
┌─────────────┐    ┌─────────────────┐    ┌──────────────────┐
│  SQS Queue  │◀───│ Polling Workers │───▶│ Processing Pool  │
└─────────────┘    │ (Configurable)  │    │ (Configurable)   │
                   └─────────────────┘    └──────────────────┘
                           │                        │
                           ▼                        ▼
                  ┌─────────────────┐    ┌──────────────────┐
                  │  Long Polling   │    │ Message Handler  │
                  │  • Batch fetch  │    │ • Error handling │
                  │  • Wait timeout │    │ • Retry logic    │
                  └─────────────────┘    └──────────────────┘
```

## Performance Considerations

### Publisher Optimization
- **Batch Size**: Increase `WithBatchMaxMessages` for higher throughput
- **Publish Interval**: Lower intervals for lower latency, higher for better batching
- **Buffer Size**: Channel buffer is automatically sized based on batch limit

### Consumer Optimization
- **Polling Workers**: Increase for better queue draining with multiple consumers
- **Processing Concurrency**: Tune based on message processing time and resources
- **Long Polling**: Use maximum `WithWaitTimeSeconds(20)` to reduce API calls
- **Batch Size**: Use maximum `WithMaxMessagesPerBatch(10)` for efficiency

## Testing

```bash
make rapid_test
make fuzz_test
make unit_test
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built on the official [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)
- Logging powered by [zerolog](https://github.com/rs/zerolog)
- Concurrency utilities from [golang.org/x/sync](https://pkg.go.dev/golang.org/x/sync)