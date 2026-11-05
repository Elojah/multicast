package multicast

import (
	"cmp"
	"context"
	"iter"
	"slices"
	"sync/atomic"

	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/pkg/atomic/slice"
	"github.com/elojah/multicast/pkg/gate"
	"github.com/elojah/multicast/subscriber"
)

// Group hands each message to every subscriber. Create one with New. It is safe for concurrent use, from handle
// included.
type Group[T any] struct {
	// subscribers is sorted by ID, so Publish gets to them in subscription order.
	subscribers slice.Slice[*subscriber.Subscriber[T]]

	// gate makes Close wait for any add under way, so Close refuses or sees each subscriber.
	gate gate.Gate

	// running counts the subscribers not done yet, plus one until the first Close. Only add raises it, under gate, so
	// it reaches 0 once, after Close, and countOut closes stopped then.
	running atomic.Int64
	stopped chan struct{}

	// ids is the last ID given out.
	ids atomic.Uint64

	// turn, set by WithSerial, holds a token while a Publish runs. A channel rather than a mutex, so that Publish calls
	// take turns in the order they come, and one waiting blocks durably in a synctest bubble.
	turn chan struct{}

	// defaults, set by WithDefaultSubscriberOptions, build each subscriber's options, which come before its own.
	defaults []func() []subscriber.Option[T]
}

// Option configures a Group, when passed to New.
type Option[T any] func(g *Group[T])

// New returns an empty Group.
func New[T any](options ...Option[T]) *Group[T] {
	g := &Group[T]{stopped: make(chan struct{})}
	g.running.Store(1)
	for _, option := range options {
		option(g)
	}

	return g
}

// WithSerial makes each Publish and PublishBatch wait, whatever its ctx, until the ones under way have returned,
// so that every subscriber takes messages in the same order, async sends aside. A Publish from a filter, or from an
// error handler a Publish runs, waits for good, and one from handle until the Publish waiting on that subscriber
// gives up.
func WithSerial[T any]() Option[T] {
	return func(g *Group[T]) {
		g.turn = make(chan struct{}, 1)
	}
}

// WithDefaultSubscriberOptions gives each subscriber the options build returns, before its Subscribe's own: those
// override a value, such as subscriber.WithBuffer's, and come after in a list, such as subscriber.WithMiddleware's.
// build runs on each Subscribe and SubscribeSeq, so that each subscriber gets its own filter or holder. Several run in
// order, and a nil build is ignored.
func WithDefaultSubscriberOptions[T any](build func() []subscriber.Option[T]) Option[T] {
	return func(g *Group[T]) {
		if build != nil {
			g.defaults = append(g.defaults, build)
		}
	}
}

// Subscribe adds a subscriber and returns its ID. Its goroutine calls handle for each message, one at a time, with the
// ID so that handle can unsubscribe itself. A Publish under way may or may not reach it. IDs count up from 1 in each
// Group, so 0 is never one.
//
// ctx is the subscription's lifetime, unless subscriber.WithDetachedContext: once it is done, the subscriber is
// unsubscribed. handle gets ctx, unless the message has its own (message.WithContext).
//
// It returns ErrClosed after Close.
func (g *Group[T]) Subscribe(ctx context.Context, handle func(ctx context.Context, id uint64, msg T) error, options ...subscriber.Option[T]) (uint64, error) {
	s, err := g.add(ctx, options)
	if err != nil {
		return 0, err
	}
	go s.Consume(handle)

	return s.ID(), nil
}

// SubscribeSeq is Subscribe with an iterator, whose loop body replaces handle, in the caller's goroutine. fail takes
// the error handle would return, and must be called before the body returns: not calling it means nil. The
// middlewares wrap the body, but a panic in it reaches the caller. Until the loop starts, Publish waits for it as
// for a busy handle.
//
// The loop ends on break, once ctx is done, or once unsubscribed and done with what it took. Ending unsubscribes, and
// reports what was taken but not yielded as a *subscriber.ClosedError. seq can be ranged over once.
//
// It returns ErrClosed after Close.
func (g *Group[T]) SubscribeSeq(ctx context.Context, options ...subscriber.Option[T]) (uint64, iter.Seq2[T, func(error)], error) {
	s, err := g.add(ctx, options)
	if err != nil {
		return 0, nil, err
	}
	id := s.ID()

	return id, s.Seq(), nil
}

// Unsubscribe removes the subscriber, or returns a *SubscriberNotFoundError. It never waits, so handle can call it:
// the subscriber may still handle what it took, and a Publish waiting on it gives up with a
// *subscriber.ClosedError. options override subscriber.WithUnsubscribeOptions. ctx is unused.
func (g *Group[T]) Unsubscribe(ctx context.Context, id uint64, options ...subscriber.UnsubscribeOption) error {
	if !g.remove(id, options...) {
		return &SubscriberNotFoundError{SubscriberID: id}
	}

	return nil
}

// Close unsubscribes every subscriber, after which Subscribe and SubscribeSeq return ErrClosed. It never waits for a
// subscriber, so handle can call it. Every call after the first returns ErrClosed.
func (g *Group[T]) Close() error {
	closed := g.gate.Close()

	// On every call, so no subscriber is left once any Close returns.
	for _, s := range g.subscribers.Clear() {
		s.Unsubscribe()
	}

	if closed {
		return ErrClosed
	}
	// Drops Close's one from running.
	g.countOut()

	return nil
}

// Shutdown is Close, then waits until every subscriber is done: it has handled or discarded what it took, and its
// subscriber.WithDone channels have received its ID or their ctx is done. It returns ctx.Err() if ctx is done first,
// else Close's error.
//
// Called from handle or an error handler, or with a SubscribeSeq loop never ranged, it waits until ctx is done. A
// Publish racing it may still report a *subscriber.ClosedError after it returns.
func (g *Group[T]) Shutdown(ctx context.Context) error {
	err := g.Close()

	// Checked first, since select picks at random among ready cases.
	select {
	case <-g.stopped:
		return err
	default:
	}

	select {
	case <-g.stopped:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Publish hands msg to every subscriber the message picks (message.WithSubscriberFilter) and whose own filter keeps
// it (subscriber.WithFilter), in subscription order, and returns how many took it, counting an async send once started.
//
// By default it waits for each subscriber in turn, and gives up on one once ctx is done or the message's timeout runs
// out, reporting why. A ready subscriber takes msg even then. ctx never reaches handle or the error handlers, but
// async sends keep using it after Publish returns.
func (g *Group[T]) Publish(ctx context.Context, msg T, options ...message.Option[T]) int {
	config := message.NewConfig(options...)

	return g.each(config, func(s *subscriber.Subscriber[T]) (bool, <-chan bool) {
		return s.Deliver(ctx, msg, config)
	})
}

// PublishBatch is Publish for every value of batch at once, with one Config: a subscriber takes the whole batch in
// one send, or misses it, and hands it to its batch handler (subscriber.WithBatchHandler), else to handle one value at
// a time. Each value counts in Stats as a message, but an error about the whole batch is reported once, with the batch
// in it.
//
// Every subscriber reads batch, at once and after an async PublishBatch returns, so batch must not change once
// passed. An empty or nil batch is not sent.
func (g *Group[T]) PublishBatch(ctx context.Context, batch message.Batch[T], options ...message.Option[T]) int {
	if batch == nil || batch.Len() == 0 {
		return 0
	}
	config := message.NewConfig(options...)

	return g.each(config, func(s *subscriber.Subscriber[T]) (bool, <-chan bool) {
		return s.DeliverBatch(ctx, batch, config)
	})
}

// Stats returns a snapshot of every subscriber's counters, in subscription order. An unsubscribed subscriber is left
// out, even while it still handles what it took. It never waits, so handle can call it.
func (g *Group[T]) Stats() []subscriber.Stats {
	subscribers := g.subscribers.Load()
	stats := make([]subscriber.Stats, 0, len(subscribers))
	for _, s := range subscribers {
		stats = append(stats, s.Stats())
	}

	return stats
}

// add creates and inserts a subscriber, or returns ErrClosed. Nothing reads its channel yet.
func (g *Group[T]) add(ctx context.Context, options []subscriber.Option[T]) (*subscriber.Subscriber[T], error) {
	// Before Enter, since build may call Subscribe or Close.
	if len(g.defaults) > 0 {
		all := make([][]subscriber.Option[T], 0, len(g.defaults)+1)
		for _, build := range g.defaults {
			all = append(all, build())
		}
		options = slices.Concat(append(all, options)...)
	}
	id := g.ids.Add(1)
	s := subscriber.New(id, options...)

	if !g.gate.Enter() {
		return nil, ErrClosed
	}
	defer g.gate.Leave()
	g.running.Add(1)

	// Before Insert, so that whoever removes the subscriber stops its ctx watch.
	ctx = s.Attach(ctx, func(options ...subscriber.UnsubscribeOption) bool { return g.remove(id, options...) }, g.countOut)
	g.subscribers.Insert(s, byID)
	// If ctx was already done, the watch may have run before Insert and found nothing.
	if ctx.Err() != nil {
		g.remove(id)
	}

	return s, nil
}

// each delivers to every subscriber config picks, and returns how many took it, waiting for each parallel send. With
// WithSerial, it first waits for its turn.
func (g *Group[T]) each(config message.Config, deliver func(s *subscriber.Subscriber[T]) (bool, <-chan bool)) int {
	if g.turn != nil {
		g.turn <- struct{}{}
		// Deferred, so that a filter's panic gives the turn back.
		defer func() { <-g.turn }()
	}

	var (
		n int
		// Whether each parallel send was taken.
		pending []<-chan bool
	)
	for _, s := range g.subscribers.Load() {
		// Load is a snapshot: a subscriber removed since is skipped, not reported.
		if s.Unsubscribed() || (config.SubscriberFilter != nil && !config.SubscriberFilter(s.ID())) {
			continue
		}
		if config.Visit != nil {
			config.Visit(s.ID())
		}
		taken, parallel := deliver(s)
		if taken {
			n++
		}
		if parallel != nil {
			pending = append(pending, parallel)
		}
	}

	// Each send gives up on its own, so this never waits for another Publish.
	for _, taken := range pending {
		if <-taken {
			n++
		}
	}

	return n
}

// remove deletes the subscriber and unsubscribes it, or returns false if there is none. Only the deleter unsubscribes,
// so racing removals do it once.
func (g *Group[T]) remove(id uint64, options ...subscriber.UnsubscribeOption) bool {
	s, ok := g.subscribers.Delete(func(s *subscriber.Subscriber[T]) bool { return s.ID() == id })
	if !ok {
		return false
	}
	s.Unsubscribe(options...)

	return true
}

// countOut drops running by one, and closes stopped at 0. Subscribers call it once done (subscriber.Subscriber.Attach).
func (g *Group[T]) countOut() {
	if g.running.Add(-1) == 0 {
		close(g.stopped)
	}
}

// byID sorts subscribers by ID.
func byID[T any](x, y *subscriber.Subscriber[T]) int {
	return cmp.Compare(x.ID(), y.ID())
}
