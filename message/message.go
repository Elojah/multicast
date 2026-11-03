// Package message holds the options passed to Publish and the Message they configure.
package message

import (
	"context"
	"time"
)

// Delivery is how Publish hands a message to a subscriber. The last option that sets it wins.
type Delivery uint8

const (
	// DeliverySync waits for each subscriber in turn. It is the default.
	DeliverySync Delivery = iota
	// DeliveryParallel sends to every subscriber at once, then waits for all of them.
	DeliveryParallel
	// DeliveryAsync sends to every subscriber at once and returns right away.
	DeliveryAsync
	// DeliveryNonBlocking drops the message for a subscriber that cannot take it right away.
	DeliveryNonBlocking
)

// Message is what one Publish sends to one subscriber: Value, or with PublishBatch, Batch.
type Message[T any] struct {
	Value T
	// Batch is nil for a single value.
	Batch  Batch[T]
	Config Config
}

// Len returns how many values m carries: its batch's, or 1.
func (m Message[T]) Len() int {
	if m.Batch != nil {
		return m.Batch.Len()
	}

	return 1
}

// Config is how one Publish sends its message to one subscriber.
type Config struct {
	Delivery Delivery

	// set holds the fields options set, which New lays over a subscriber's defaults even when zero. Next to Delivery, so
	// both fit in one word: every async send copies Config to the heap.
	set fields

	// Timeout bounds the wait for the subscriber to take the message. 0 means none.
	Timeout time.Duration

	// ErrorHandler gets every error about this message, after the subscriber's own. nil means none.
	ErrorHandler func(ctx context.Context, err error)

	// Context replaces the subscriber's ctx for this message. nil means none.
	Context context.Context //nolint:containedctx // it travels with the message, which outlives Publish

	// SubscriberFilter picks, by ID, the subscribers Publish hands the message to. nil means every one. Only a
	// Publish's own is read.
	SubscriberFilter func(id uint64) bool

	// Visit gets the ID of each subscriber SubscriberFilter picks, before the subscriber's own filters. nil means none.
	// Only a Publish's own is read.
	Visit func(id uint64)
}

// fields is a set of Config fields.
type fields uint8

const (
	fieldDelivery fields = 1 << iota
	fieldTimeout
	fieldErrorHandler
	fieldContext
)

// NewConfig applies options once, for New to lay over each subscriber's defaults.
func NewConfig[T any](options ...Option[T]) Config {
	// Before declaring config, which the options move to the heap.
	if len(options) == 0 {
		return Config{}
	}
	var config Config
	for _, option := range options {
		option(&config)
	}

	return config
}

// New returns value with the fields config's options set laid over a subscriber's defaults. Both are copies, so
// nothing escapes.
func New[T any](value T, defaults, config Config) Message[T] {
	return Message[T]{Value: value, Config: defaults.with(config)}
}

// NewBatch is New for a batch.
func NewBatch[T any](batch Batch[T], defaults, config Config) Message[T] {
	return Message[T]{Batch: batch, Config: defaults.with(config)}
}

// with returns defaults with the fields config's options set laid over them.
func (defaults Config) with(config Config) Config {
	if config.set&fieldDelivery != 0 {
		defaults.Delivery = config.Delivery
	}
	if config.set&fieldTimeout != 0 {
		defaults.Timeout = config.Timeout
	}
	if config.set&fieldErrorHandler != 0 {
		defaults.ErrorHandler = config.ErrorHandler
	}
	if config.set&fieldContext != 0 {
		defaults.Context = config.Context
	}

	return defaults
}
