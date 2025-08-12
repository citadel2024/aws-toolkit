package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/citadel2024/aws-toolkit/sqsx"
	"github.com/citadel2024/aws-toolkit/sqsx/consumer"
	"github.com/rs/zerolog"
)

// OrderMessage represents a sample message structure
type OrderMessage struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
}

// PaymentMessage represents another sample message structure
type PaymentMessage struct {
	PaymentID string  `json:"payment_id"`
	OrderID   string  `json:"order_id"`
	Amount    float64 `json:"amount"`
	Method    string  `json:"method"`
}

func main() {
	advancedExample()
}

// advancedExample demonstrates advanced configuration and structured message handling
func advancedExample() {
	logger := zerolog.New(os.Stdout).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Str("service", "order-processor").
		Logger()

	// Queue URL for order processing
	queueURL := "https://sqs.us-east-1.amazonaws.com/123456789012/order-processing-queue"

	// Advanced message handler with structured message processing
	messageHandler := func(ctx context.Context, msg *types.Message) error {
		msgLogger := logger.With().Str("messageId", *msg.MessageId).Logger()
		msgLogger.Debug().Msg("Starting message processing")

		// Extract message attributes for routing
		messageType := "unknown"
		if attr, exists := msg.MessageAttributes["MessageType"]; exists && attr.StringValue != nil {
			messageType = *attr.StringValue
		}

		// Route message based on type
		switch messageType {
		case "order":
			return processOrderMessage(ctx, msg, msgLogger)
		case "payment":
			return processPaymentMessage(ctx, msg, msgLogger)
		default:
			return processGenericMessage(ctx, msg, msgLogger)
		}
	}

	// Exception handler for logging unhandled errors
	exceptionHandler := func(ctx context.Context, err error) {
		logger.Error().
			Err(err).
			Str("component", "sqs-consumer").
			Msg("Unhandled exception occurred")

		// Here you could send metrics, alerts, etc.
	}

	// Shutdown hook for cleanup
	shutdownHook := func() {
		logger.Info().Msg("Performing cleanup operations...")
		// Cleanup resources, flush metrics, etc.
		time.Sleep(100 * time.Millisecond)
		logger.Info().Msg("Cleanup completed")
	}

	// Create consumer with advanced configuration
	sqsConsumer, err := consumer.New(queueURL,
		consumer.WithMessageHandler(messageHandler),
		consumer.WithExceptionHandler(exceptionHandler),
		consumer.WithLogger(logger),
		consumer.WithPollingGoroutines(3),      // 3 polling goroutines
		consumer.WithProcessingConcurrency(10), // Process up to 10 messages concurrently
		consumer.WithMaxMessagesPerBatch(10),   // Receive up to 10 messages per batch
		consumer.WithWaitTimeSeconds(20),       // Use long polling (20 seconds)
		consumer.WithShutdownHook(shutdownHook),
	)
	if err != nil {
		log.Fatalf("Failed to create advanced consumer: %v", err)
	}

	// Set up context with timeout for demo purposes
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Handle shutdown signals
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-sigChan:
			logger.Info().Msg("Shutdown signal received")
			cancel()
		case <-ctx.Done():
			logger.Info().Msg("Context timeout reached")
		}
	}()

	// Start the consumer
	logger.Info().Msg("Starting advanced consumer...")
	if err := sqsConsumer.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("Advanced consumer stopped with error")
	} else {
		logger.Info().Msg("Advanced consumer stopped gracefully")
	}
}

// processOrderMessage handles order-specific message processing
func processOrderMessage(ctx context.Context, msg *types.Message, logger zerolog.Logger) error {
	var order OrderMessage
	if err := json.Unmarshal([]byte(*msg.Body), &order); err != nil {
		logger.Error().Err(err).Msg("Failed to unmarshal order message")
		return fmt.Errorf("invalid order message format: %w", sqsx.ErrNonRetryable)
	}

	logger.Info().
		Str("orderId", order.OrderID).
		Str("customerId", order.CustomerID).
		Float64("amount", order.Amount).
		Str("status", order.Status).
		Msg("Processing order")

	// Validate order
	if order.OrderID == "" || order.CustomerID == "" {
		logger.Error().Msg("Order missing required fields")
		return fmt.Errorf("invalid order data: %w", sqsx.ErrNonRetryable)
	}

	// Simulate order processing
	if err := processOrder(ctx, &order); err != nil {
		logger.Error().Err(err).Msg("Failed to process order")
		return err // Retryable error
	}

	logger.Info().Str("orderId", order.OrderID).Msg("Order processed successfully")
	return nil
}

// processPaymentMessage handles payment-specific message processing
func processPaymentMessage(ctx context.Context, msg *types.Message, logger zerolog.Logger) error {
	var payment PaymentMessage
	if err := json.Unmarshal([]byte(*msg.Body), &payment); err != nil {
		logger.Error().Err(err).Msg("Failed to unmarshal payment message")
		return fmt.Errorf("invalid payment message format: %w", sqsx.ErrNonRetryable)
	}

	logger.Info().
		Str("paymentId", payment.PaymentID).
		Str("orderId", payment.OrderID).
		Float64("amount", payment.Amount).
		Str("method", payment.Method).
		Msg("Processing payment")

	// Simulate payment processing
	if err := processPayment(ctx, &payment); err != nil {
		logger.Error().Err(err).Msg("Failed to process payment")
		return err // Retryable error
	}

	logger.Info().Str("paymentId", payment.PaymentID).Msg("Payment processed successfully")
	return nil
}

// processGenericMessage handles unknown message types
func processGenericMessage(ctx context.Context, msg *types.Message, logger zerolog.Logger) error {
	logger.Warn().
		Str("body", *msg.Body).
		Msg("Processing generic message")

	// Log the message for manual inspection
	logger.Info().Msg("Generic message logged for manual review")
	return nil
}

func processOrder(ctx context.Context, order *OrderMessage) error {
	// Simulate database operations, external API calls, etc.
	time.Sleep(200 * time.Millisecond)
	// Simulate occasional failures
	if order.Amount < 0 {
		return fmt.Errorf("invalid order amount: %f", order.Amount)
	}

	return nil
}

func processPayment(ctx context.Context, payment *PaymentMessage) error {
	// Simulate payment gateway interaction
	time.Sleep(150 * time.Millisecond)
	// Simulate payment failures
	if payment.Amount <= 0 {
		return fmt.Errorf("invalid payment amount: %f", payment.Amount)
	}

	return nil
}
