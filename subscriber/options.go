package subscriber

import (
	"context"
	"iter"
	"time"

	"github.com/elojah/multicast/message"
)

// Option configures a subscriber, when passed to Subscribe or SubscribeSeq.
type Option[T any] func(config *config[T])

// UnsubscribeOption configures an Unsubscribe, or every one with WithUnsubscribeOptions.
type UnsubscribeOption func(unsubscription *unsubscription)

// config is what Options set.
type config[T any] struct {
	buffer              int
	defaults            message.Config
	unsubscribeDefaults unsubscription
	detached            bool
	store               Store[T]
	middlewares         []Middleware[T]
	asyncLimit          int
	replay              iter.Seq[T]
	holder              Holder[T]

	// batchHandler is WithBatchHandler's handle, wrapped in its middlewares, then once New has applied every option, in
	// batchMiddlewares.
	batchHandler     Handler[message.Batch[T]]
	batchMiddlewares []Middleware[message.Batch[T]]

	errorHandler func(ctx context.Context, err error)
	filter       func(msg T) bool
	evict        func(err error) bool

	done []ctxchan
}

// ctxchan is a WithDone channel, and the ctx that bounds the send on it.
type ctxchan struct {
	ctx context.Context //nolint:containedctx // bounds a send at the end of the subscription
	ch  chan<- uint64
}

// unsubscription is what UnsubscribeOptions set.
type unsubscription struct {
	discard bool
}

// WithBuffer sets the subscriber's buffer size: Publish waits for it only once the buffer is full. A batch takes one
// place, whatever its size.
func WithBuffer[T any](buffer int) Option[T] {
	return func(config *config[T]) {
		config.buffer = buffer
	}
}

// WithErrorHandler sets the handler for every error about the subscriber's messages: handle's, and why Publish could
// not hand one over. It runs where the error happens, in the subscriber's goroutine or Publish's, and holds that up.
// Without one, errors are discarded.
func WithErrorHandler[T any](handler func(ctx context.Context, err error)) Option[T] {
	return func(config *config[T]) {
		config.errorHandler = handler
	}
}

// WithDeadLetters puts every message the subscriber loses in store, before the error handlers run, so each message is
// handled or stored, once. Put runs where the error handlers do. If it fails, they get a *StoreError.
func WithDeadLetters[T any](store Store[T]) Option[T] {
	return func(config *config[T]) {
		config.store = store
	}
}

// WithMiddleware appends middlewares around handle, or a SubscribeSeq loop body, the first one outermost.
func WithMiddleware[T any](middlewares ...Middleware[T]) Option[T] {
	return func(config *config[T]) {
		config.middlewares = append(config.middlewares, middlewares...)
	}
}

// WithBatchHandler hands each batch the subscriber takes (Group.PublishBatch, or a holder's, such as
// holder.NewBatch) to handle, wrapped in middlewares, the first one outermost, rather than each of its values to handle
// or the loop body, through WithMiddleware's. handle gets the ctx its values would, and its error counts every value
// as failed but is reported once. With SubscribeSeq, handle runs in the subscriber's goroutine, not the loop's. A nil
// handle is ignored.
func WithBatchHandler[T any](handle Handler[message.Batch[T]], middlewares ...Middleware[message.Batch[T]]) Option[T] {
	return func(config *config[T]) {
		if handle != nil {
			config.batchHandler = chain(handle, middlewares...)
		}
	}
}

// WithBatchMiddleware appends middlewares around the batch handler and its own middlewares (WithBatchHandler), the
// first one outermost, whichever option comes first, so that one option can wrap a batch handler another sets. Without
// a batch handler, they never run.
func WithBatchMiddleware[T any](middlewares ...Middleware[message.Batch[T]]) Option[T] {
	return func(config *config[T]) {
		config.batchMiddlewares = append(config.batchMiddlewares, middlewares...)
	}
}

// WithDetachedContext keeps the subscriber once its ctx is done: it runs with context.WithoutCancel(ctx).
func WithDetachedContext[T any]() Option[T] {
	return func(config *config[T]) {
		config.detached = true
	}
}

// WithDefaultMessageOptions sets default message options, which a Publish's own override.
func WithDefaultMessageOptions[T any](options ...message.Option[T]) Option[T] {
	return func(config *config[T]) {
		for _, option := range options {
			option(&config.defaults)
		}
	}
}

// WithUnsubscribeOptions sets default unsubscribe options, which Unsubscribe's own override. Close and a done ctx
// apply only these.
func WithUnsubscribeOptions[T any](options ...UnsubscribeOption) Option[T] {
	return func(config *config[T]) {
		for _, option := range options {
			option(&config.unsubscribeDefaults)
		}
	}
}

// WithTimeout is WithDefaultMessageOptions(message.WithTimeout(timeout)).
func WithTimeout[T any](timeout time.Duration) Option[T] {
	return func(config *config[T]) {
		config.defaults.Timeout = timeout
	}
}

// WithEvict unsubscribes the subscriber, with WithUnsubscribeDiscard, on the first error evict returns true for: a
// *TimeoutError or *DroppedError, from Publish's goroutine, or a holder's *DroppedError or handle's error past every
// middleware, from the subscriber's. evict may run concurrently, and never gets a *ClosedError. A nil evict never
// evicts. The error handlers get the evicting error as an *EvictedError.
func WithEvict[T any](evict func(err error) bool) Option[T] {
	return func(config *config[T]) {
		config.evict = evict
	}
}

// WithFilter skips every message keep rejects, before anything else, so the subscriber never wakes up for it, and it
// is neither reported nor counted. keep runs in Publish's goroutine, so it may run concurrently. Several WithFilter
// run in order until one rejects. keep sees each value of a batch once: of a batch it keeps part of, the subscriber
// takes a batch from message.Batch.New holding what it keeps. A nil keep is ignored.
func WithFilter[T any](keep func(msg T) bool) Option[T] {
	return func(config *config[T]) {
		if keep == nil {
			return
		}
		previous := config.filter
		if previous == nil {
			config.filter = keep

			return
		}
		config.filter = func(msg T) bool { return previous(msg) && keep(msg) }
	}
}

// WithAsyncLimit bounds the async sends (message.WithAsync) under way to the subscriber at n: past n, an async message
// is dropped with a *DroppedError. n <= 0, the default, means no limit.
func WithAsyncLimit[T any](n int) Option[T] {
	return func(config *config[T]) {
		config.asyncLimit = n
	}
}

// WithReplay makes the subscriber handle each value of replay, such as its dead letters from before a restart, before
// any message. Each value is handled, reported and counted like a message, from the subscriber's goroutine, and a
// Publish meanwhile waits as for a busy handle.
//
// yield returns once the value is handled or reported, so replay can ack it then, and returns false once the
// subscriber discards. A value lost again goes back to the dead letters, so a replay of them should yield only what
// they held when it started.
func WithReplay[T any](replay iter.Seq[T]) Option[T] {
	return func(config *config[T]) {
		config.replay = replay
	}
}

// WithHolder passes each message the subscriber takes to h, a batch one value at a time, and each one h releases, once
// due, to handle and its middlewares, or to the loop body. Once unsubscribed, the subscriber handles everything h
// still holds, or reports it with WithUnsubscribeDiscard, so Shutdown waits for it. A message h has no room for is lost
// as a *DroppedError. Replayed values skip h. Give each subscriber its own. A nil h is ignored.
func WithHolder[T any](h Holder[T]) Option[T] {
	return func(config *config[T]) {
		if h != nil {
			config.holder = h
		}
	}
}

// WithDone sends the subscriber's ID on done once it is unsubscribed, whatever removed it, and done with what it took:
// after the last call to handle or the loop body. The send waits for a receiver until ctx is done, and Shutdown waits
// for it, so subscribers can share done. It is never sent for a SubscribeSeq loop never ranged. Several are sent in
// order, and a nil done is ignored.
func WithDone[T any](ctx context.Context, done chan<- uint64) Option[T] {
	return func(config *config[T]) {
		if done != nil {
			config.done = append(config.done, ctxchan{ctx: ctx, ch: done})
		}
	}
}

// WithUnsubscribeDiscard makes the subscriber report what it has left as *ClosedError instead of handling it, and ends
// a SubscribeSeq loop. A handle already running finishes. It has no effect on a subscriber already unsubscribed.
func WithUnsubscribeDiscard() UnsubscribeOption {
	return func(unsubscription *unsubscription) {
		unsubscription.discard = true
	}
}

// WithUnsubscribeDeliver makes the subscriber handle what it has left. It is the default, so it only overrides
// WithUnsubscribeOptions.
func WithUnsubscribeDeliver() UnsubscribeOption {
	return func(unsubscription *unsubscription) {
		unsubscription.discard = false
	}
}
