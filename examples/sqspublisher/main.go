package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/citadel2024/aws-toolkit/sqsx"
	"github.com/citadel2024/aws-toolkit/sqsx/publisher"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// OrderEvent represents a business event
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	EventType  string    `json:"event_type"`
	Amount     float64   `json:"amount"`
	Currency   string    `json:"currency"`
	Timestamp  time.Time `json:"timestamp"`
}

// EventPublisher manages SQS event publishing
type EventPublisher struct {
	publisher   sqsx.Publisher
	logger      zerolog.Logger
	eventBuffer chan OrderEvent
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewEventPublisher creates a new event publisher
func NewEventPublisher(queueURL string, workerCount int) (*EventPublisher, error) {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		With().Str("component", "event-publisher").Logger()

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	sqsClient := sqs.NewFromConfig(cfg)
	pub := publisher.New(queueURL,
		publisher.WithLogger(logger),
		publisher.WithClient(sqsClient),
		publisher.WithBatchMaxMessages(10),
		publisher.WithPublishInterval(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	return &EventPublisher{
		publisher:   pub,
		logger:      logger,
		eventBuffer: make(chan OrderEvent, 1000),
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Start begins the event publishing process
func (ep *EventPublisher) Start() error {
	if err := ep.publisher.Start(ep.ctx); err != nil {
		return fmt.Errorf("failed to start publisher: %w", err)
	}

	ep.logger.Info().Msg("Starting event publisher")
	for i := 0; i < 3; i++ {
		ep.wg.Add(1)
		go ep.worker(i)
	}
	return nil
}

// PublishEvent queues an event for publishing
func (ep *EventPublisher) PublishEvent(event OrderEvent) error {
	select {
	case ep.eventBuffer <- event:
		return nil
	case <-ep.ctx.Done():
		return fmt.Errorf("publisher is shutting down")
	default:
		return fmt.Errorf("event buffer is full")
	}
}

// worker processes events from the buffer
func (ep *EventPublisher) worker(workerID int) {
	defer ep.wg.Done()
	logger := ep.logger.With().Int("worker", workerID).Logger()
	logger.Info().Msg("Worker started")

	for {
		select {
		case event := <-ep.eventBuffer:
			if err := ep.processEvent(event); err != nil {
				logger.Error().Err(err).Str("orderId", event.OrderID).Msg("Failed to process event")
			}
		case <-ep.ctx.Done():
			logger.Info().Msg("Worker shutting down")
			return
		}
	}
}

// processEvent converts and publishes an event
func (ep *EventPublisher) processEvent(event OrderEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	entry := types.SendMessageBatchRequestEntry{
		Id:          aws.String(uuid.New().String()),
		MessageBody: aws.String(string(eventJSON)),
	}
	return ep.publisher.PublishMessageBatchWithEntry(ep.ctx, entry)
}

// Shutdown gracefully shuts down the event publisher
func (ep *EventPublisher) Shutdown(timeout time.Duration) error {
	ep.logger.Info().Msg("Initiating shutdown")
	ep.cancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		ep.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		ep.logger.Info().Msg("All workers stopped")
	case <-shutdownCtx.Done():
		ep.logger.Warn().Msg("Shutdown timeout reached")
		return fmt.Errorf("shutdown timeout exceeded")
	}

	if err := ep.publisher.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return nil
}

// generateTestEvents simulates event generation
func generateTestEvents(ep *EventPublisher, eventCount int) {
	for i := 0; i < eventCount; i++ {
		event := OrderEvent{
			OrderID:    fmt.Sprintf("order-%d", i),
			CustomerID: fmt.Sprintf("customer-%d", i%100),
			EventType:  []string{"order_created", "order_updated"}[i%2],
			Amount:     float64(10 + i%1000),
			Currency:   []string{"USD", "EUR"}[i%2],
			Timestamp:  time.Now(),
		}

		if err := ep.PublishEvent(event); err != nil {
			log.Error().Err(err).Int("eventIndex", i).Msg("Failed to queue event")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	queueURL := os.Getenv("SQS_QUEUE_URL")
	if queueURL == "" {
		log.Fatal().Msg("SQS_QUEUE_URL environment variable is required")
	}

	eventPublisher, err := NewEventPublisher(queueURL, 3)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create event publisher")
	}

	if err := eventPublisher.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start event publisher")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go generateTestEvents(eventPublisher, 1000)

	<-sigChan
	log.Info().Msg("Received shutdown signal")

	if err := eventPublisher.Shutdown(30 * time.Second); err != nil {
		log.Error().Err(err).Msg("Error during shutdown")
		os.Exit(1)
	}
}
