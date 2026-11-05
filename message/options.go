package message

import (
	"context"
	"math"
	"time"
)

// Option configures a message, when passed to Publish or to subscriber.WithDefaultMessageOptions.
type Option[T any] func(config *Config)

// WithSync makes Publish wait for each subscriber in turn. It is the default, so it only overrides a subscriber's
// default delivery.
func WithSync[T any]() Option[T] {
	return func(config *Config) {
		config.Delivery = DeliverySync
		config.set |= fieldDelivery
	}
}

// WithParallel makes Publish send to every subscriber at once, then wait for all of them. A slow subscriber holds up
// Publish but no other subscriber. Messages from one goroutine stay in order.
func WithParallel[T any]() Option[T] {
	return func(config *Config) {
		config.Delivery = DeliveryParallel
		config.set |= fieldDelivery
	}
}

// WithAsync makes Publish send to each subscriber from a goroutine of its own and return at once. Messages may arrive
// out of order, and each send holds a goroutine until taken (see subscriber.WithAsyncLimit).
//
// The sends keep using the Publish ctx, so cancelling it drops every message not taken yet. To outlive the caller:
//
//	detached := context.WithoutCancel(ctx)
//	g.Publish(detached, msg, message.WithAsync[T](), message.WithTimeout[T](time.Second), message.WithContext[T](detached))
func WithAsync[T any]() Option[T] {
	return func(config *Config) {
		config.Delivery = DeliveryAsync
		config.set |= fieldDelivery
	}
}

// WithNonBlocking makes Publish never wait: a busy subscriber, or one with a full buffer, misses the message with a
// *subscriber.DroppedError.
func WithNonBlocking[T any]() Option[T] {
	return func(config *Config) {
		config.Delivery = DeliveryNonBlocking
		config.set |= fieldDelivery
	}
}

// WithContext gives handle, its middlewares and the error handlers ctx for this message, instead of the subscription's.
// It is used as is, done or not: pass context.WithoutCancel(ctx) to keep only its values. nil overrides a default.
func WithContext[T any](ctx context.Context) Option[T] {
	return func(config *Config) {
		config.Context = ctx
		config.set |= fieldContext
	}
}

// WithErrorHandler adds a handler for every error about this message, after the subscriber's own. Every subscriber
// shares it, so it may run concurrently, and after Publish returns.
func WithErrorHandler[T any](handler func(ctx context.Context, err error)) Option[T] {
	return func(config *Config) {
		config.ErrorHandler = handler
		config.set |= fieldErrorHandler
	}
}

// WithTimeout bounds the wait for each subscriber, from when Publish gets to it, whereas a ctx deadline is shared.
// A subscriber that misses it gets a *subscriber.TimeoutError. 0 or less means none, overriding subscriber.WithTimeout.
func WithTimeout[T any](timeout time.Duration) Option[T] {
	return func(config *Config) {
		config.Timeout = timeout
		config.set |= fieldTimeout
	}
}

// WithLimit makes Publish stop once n subscribers have taken the message, so it reaches the first n in subscription
// order that take it: one whose filters reject it, or that misses it unsubscribed, timed out or busy, hands over to
// the next. One that takes it then fails to handle it does not. With WithParallel or WithAsync, a send counts once
// started. 0 or less means no limit, the default, and n in subscriber.WithDefaultMessageOptions is ignored.
func WithLimit[T any](n int) Option[T] {
	return func(config *Config) {
		config.Limit = uint32(min(uint64(max(n, 0)), math.MaxUint32)) //nolint:gosec // clamped to uint32
	}
}

// WithSubscriberFilter hands the message only to the subscribers keep picks. Publish calls keep from its goroutine,
// once per subscriber, before the subscriber's own filters (subscriber.WithFilter), so a skipped subscriber is neither
// waited for, reported nor counted, and its filters never see the message. Several WithSubscriberFilter run in order
// until one rejects. A nil keep is ignored, and so is keep in subscriber.WithDefaultMessageOptions.
func WithSubscriberFilter[T any](keep func(id uint64) bool) Option[T] {
	return func(config *Config) {
		if keep == nil {
			return
		}
		previous := config.SubscriberFilter
		if previous == nil {
			config.SubscriberFilter = keep

			return
		}
		config.SubscriberFilter = func(id uint64) bool { return previous(id) && keep(id) }
	}
}

// WithVisit calls visit with the ID of each subscriber Publish gets to, from its goroutine, once WithSubscriberFilter
// picks it and before the subscriber's own filters (subscriber.WithFilter) see the message. Several WithVisit run in
// order. A nil visit is ignored, and so is visit in subscriber.WithDefaultMessageOptions.
func WithVisit[T any](visit func(id uint64)) Option[T] {
	return func(config *Config) {
		if visit == nil {
			return
		}
		previous := config.Visit
		if previous == nil {
			config.Visit = visit

			return
		}
		config.Visit = func(id uint64) {
			previous(id)
			visit(id)
		}
	}
}
