// Package subscriber holds the subscriber options, Handler and Middleware, Store, Stats, the errors about messages, and
// the Subscriber a Group manages.
package subscriber

import (
	"context"
	"sync/atomic"
	"time"
)

// Subscriber is one subscription of a Group. Only Consume reads its channel.
type Subscriber[T any] struct {
	id     uint64
	config config[T]
	inbox  inbox[T]

	// ctx is for every message without one of its own. Set by Attach.
	ctx context.Context //nolint:containedctx // the subscription's, which outlives every call

	// remove removes the subscriber from its Group, and reports whether this call did. Set by Attach.
	remove func(options ...UnsubscribeOption) bool

	// finished counts the subscriber out of its Group. Set by Attach.
	finished func()

	// stopContextLifetime cancels the AfterFunc set by Attach.
	stopContextLifetime func() bool

	// discarding makes Consume report messages instead of handling them, and ends a Seq loop.
	discarding atomic.Bool

	counters counters
}

// New returns a subscriber holding the subscription's reference, which Unsubscribe drops.
func New[T any](id uint64, options ...Option[T]) *Subscriber[T] {
	var config config[T]
	for _, option := range options {
		option(&config)
	}
	if config.batchHandler != nil {
		config.batchHandler = chain(config.batchHandler, config.batchMiddlewares...)
	}
	s := &Subscriber[T]{id: id, config: config, counters: counters{created: time.Now()}}
	s.inbox.open(config.buffer)

	return s
}

// ID returns the subscriber's ID.
func (s *Subscriber[T]) ID() uint64 {
	return s.id
}

// Attach sets the subscriber's ctx, and its Group's remove, called once ctx is done, when a Seq loop ends and to
// evict, and finished, called once the subscriber is done, which must not block. It must be called once, first, and
// returns the subscriber's ctx: context.WithoutCancel(ctx) with WithDetachedContext.
func (s *Subscriber[T]) Attach(ctx context.Context, remove func(options ...UnsubscribeOption) bool, finished func()) context.Context {
	if s.config.detached {
		ctx = context.WithoutCancel(ctx)
	}
	s.ctx = ctx
	s.remove = remove
	s.finished = finished
	// Never runs once detached, since ctx is never done.
	s.stopContextLifetime = context.AfterFunc(ctx, func() { remove() })

	return ctx
}

// Unsubscribe drops the subscription's reference, with the unsubscribe defaults then options. Only whoever removed the
// subscriber calls it, once.
func (s *Subscriber[T]) Unsubscribe(options ...UnsubscribeOption) {
	u := s.config.unsubscribeDefaults
	for _, option := range options {
		option(&u)
	}
	// Before stop, which may close ch right away: the reader must see it for every buffered message.
	if u.discard {
		s.discarding.Store(true)
	}
	// However it was removed, so the AfterFunc does not outlive it.
	s.stopContextLifetime()
	s.inbox.stop()
}
