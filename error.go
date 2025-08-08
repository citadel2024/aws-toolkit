package main

import "errors"

var (
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
