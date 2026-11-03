// Package reply asks a Group's subscribers a query and gathers their answers, such as each sensor's latest reading:
//
//	g := reply.New[string, float64]()
//	id, err := g.Subscribe(ctx, func(ctx context.Context, _ uint64, unit string) (float64, error) {
//		return read(ctx, unit)
//	})
//	...
//	ctx, cancel := context.WithTimeout(ctx, time.Second)
//	defer cancel()
//	results := g.Publish(ctx, "celsius")
//
// A Group is a multicast.Group of Requests, which are messages like any other, so every option applies as it is.
// Publish waits until each subscriber the request reached has answered, with a value or an error, why the library lost
// the request included, or until ctx is done.
//
// Warning: use FirstN from package reply/middleware and NewLatest from package reply/holder, not those of packages
// middleware and holder, as package middleware explains.
package reply

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/elojah/multicast"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/subscriber"
)

// Group is a multicast.Group of Requests, whose Subscribe and SubscribeSeq answer them and whose Publish waits for the
// answers. PublishBatch and g.Group.Publish drop the answers, and Publish never waits for a subscriber added through
// g.Group. To wrap a Group made elsewhere:
//
//	answering := &reply.Group[Q, R]{Group: g}
type Group[Q, R any] struct {
	*multicast.Group[Request[Q, R]]
}

// New returns an empty Group, as multicast.New does.
func New[Q, R any](options ...multicast.Option[Request[Q, R]]) *Group[Q, R] {
	return &Group[Q, R]{Group: multicast.New(options...)}
}

// Subscribe is multicast.Group.Subscribe with a handle that returns its answer. A value answers once the middlewares in
// options return nil, and an error once reported, with the zero value, so that a default middleware
// (multicast.WithDefaultSubscriberOptions), such as Retry or Recover, answers the attempt that ended.
//
// Past options, an error the library did not make becomes a *subscriber.HandleError, so middleware.WrapError is not
// needed. So does a batch handler's, about the batch, which answers each of its requests not answered yet.
//
// A request stays unanswered if a default middleware returns nil on an error, or an error of its own naming no
// subscriber.
func (g *Group[Q, R]) Subscribe(
	ctx context.Context, handle func(ctx context.Context, id uint64, query Q) (R, error),
	options ...subscriber.Option[Request[Q, R]],
) (uint64, error) {
	// last is what handle returned for the request under way, in the subscriber's goroutine.
	var last, zero R
	inner := func(ctx context.Context, id uint64, req Request[Q, R]) error {
		value, err := handle(ctx, id, req.Query)
		last = value

		return err
	}
	answer := func(next subscriber.Handler[Request[Q, R]]) subscriber.Handler[Request[Q, R]] {
		return func(ctx context.Context, id uint64, req Request[Q, R]) error {
			// So that a middleware returning nil without calling next answers the zero value.
			last = zero
			if err := next(ctx, id, req); err != nil {
				return attribute(id, req, err)
			}
			req.answer(id, Result[R]{Value: last})

			return nil
		}
	}

	return g.Group.Subscribe(ctx, inner, answering(answer, options)...)
}

// SubscribeSeq is multicast.Group.SubscribeSeq for a loop answering each request with Request.Answer or fail. fail's
// error, and a batch handler's, answers as with Subscribe.
func (g *Group[Q, R]) SubscribeSeq(
	ctx context.Context, options ...subscriber.Option[Request[Q, R]],
) (uint64, iter.Seq2[Request[Q, R], func(error)], error) {
	attributing := func(next subscriber.Handler[Request[Q, R]]) subscriber.Handler[Request[Q, R]] {
		return func(ctx context.Context, id uint64, req Request[Q, R]) error {
			if err := next(ctx, id, req); err != nil {
				return attribute(id, req, err)
			}

			return nil
		}
	}

	return g.Group.SubscribeSeq(ctx, answering(attributing, options)...)
}

// Publish publishes a Request carrying query, as multicast.Group.Publish does with options, then waits until each
// subscriber the request reached has answered, or until ctx is done. It returns their Results by ID: an answer, the
// error the library reported on losing the request, such as a *subscriber.DroppedError, or once ctx is done a
// *subscriber.TimeoutError wrapping ctx.Err().
//
// It sets the message's error handler, which calls the one in options first, so a subscriber's default one
// (subscriber.WithDefaultMessageOptions) does not run. Called from handle, Publish waits for that subscriber until ctx
// is done, since it answers only once handle returns.
//
// Publish waits for a holder's window, which serial Publishes from one goroutine never share, and for
// middleware.Retry's attempts, which follow handle's ctx: pass ctx with message.WithContext to end them with Publish.
func (g *Group[Q, R]) Publish(ctx context.Context, query Q, options ...message.Option[Request[Q, R]]) map[uint64]Result[R] {
	c := &collector[Q, R]{answered: make(chan struct{}, 1)}
	req := Request[Q, R]{Query: query, to: c}
	handler := message.NewConfig(options...).ErrorHandler
	options = append(slices.Clip(options),
		message.WithVisit[Request[Q, R]](c.at.Store),
		message.WithErrorHandler[Request[Q, R]](func(ctx context.Context, err error) {
			if handler != nil {
				handler(ctx, err)
			}
			fail[Q, R](err)
		}),
	)
	g.Group.Publish(ctx, req, options...)

	return c.wait(ctx, req)
}

// Request is a message carrying Query, which subscribers answer. Only Group.Publish sets where answers go: those to a
// request published otherwise, replayed or read back from a store are dropped.
type Request[Q, R any] struct {
	Query Q

	// to is the collector of the Publish that sent the request, and merged the requests merged into it.
	to     *collector[Q, R]
	merged []Request[Q, R]
}

// Answer sends id's answer to the Publish that sent r and those of the requests merged into it, for a SubscribeSeq loop
// or a batch handler. It never blocks. Only a subscriber's first answer counts, and one after Publish returns is
// dropped.
func (r Request[Q, R]) Answer(id uint64, value R, err error) {
	r.answer(id, Result[R]{Value: value, Err: err})
}

// Merge returns r, also answering for older, for a holder dropping older, which would otherwise stay unanswered. The
// request returned keeps older, its Query included.
func (r Request[Q, R]) Merge(older Request[Q, R]) Request[Q, R] {
	// Clipped, so that requests merged from the same r never share an array.
	r.merged = append(slices.Clip(r.merged), older)

	return r
}

// Hold makes the Publish that sent r and those of the requests merged into it wait for the matching Release, for a
// middleware that may Withdraw a subscriber once it has answered. Call it before that subscriber answers. It never
// blocks.
func (r Request[Q, R]) Hold() {
	r.each((*collector[Q, R]).hold)
}

// Release ends one Hold of r. It never blocks.
func (r Request[Q, R]) Release() {
	r.each((*collector[Q, R]).settle)
}

// Withdraw makes the Publish that sent r and those of the requests merged into it neither wait for id nor return it,
// even if it has answered, and drops any later answer. It never blocks.
func (r Request[Q, R]) Withdraw(id uint64) {
	r.each(func(c *collector[Q, R]) { c.withdraw(id) })
}

func (r Request[Q, R]) answer(id uint64, result Result[R]) {
	if r.to != nil {
		r.to.answer(id, result)
	}
	for _, older := range r.merged {
		older.answer(id, result)
	}
}

// each calls f with the collector of each Publish r answers.
func (r Request[Q, R]) each(f func(c *collector[Q, R])) {
	if r.to != nil {
		f(r.to)
	}
	for _, older := range r.merged {
		older.each(f)
	}
}

// Result is one subscriber's answer: Value, unless Err.
type Result[R any] struct {
	Value R
	Err   error
}

// collector gathers the answers to one Publish, which come from Publish's goroutine and every subscriber's.
type collector[Q, R any] struct {
	// results holds a *Result[R] for each subscriber the request reached, nil until it answers. wait empties it.
	results sync.Map
	// reached counts the subscribers reached, and waiting those yet to answer, and each hold.
	reached atomic.Int64
	waiting atomic.Int64
	// answered wakes wait once waiting reaches 0. Buffered, so that settle never blocks.
	answered chan struct{}

	// at is the subscriber Publish is at, set by its visit for reach, which that subscriber's filters call right after.
	at atomic.Uint64
}

// reach makes Publish wait for the subscriber it is at.
func (c *collector[Q, R]) reach() {
	if _, loaded := c.results.LoadOrStore(c.at.Load(), (*Result[R])(nil)); !loaded {
		c.reached.Add(1)
		c.waiting.Add(1)
	}
}

// answer keeps id's result, if Publish waits for it and it has not answered yet. It never blocks.
func (c *collector[Q, R]) answer(id uint64, result Result[R]) {
	if c.results.CompareAndSwap(id, (*Result[R])(nil), &result) {
		c.settle()
	}
}

// withdraw takes id out of Publish, so that any later answer fails its CompareAndSwap.
func (c *collector[Q, R]) withdraw(id uint64) {
	value, loaded := c.results.LoadAndDelete(id)
	if !loaded {
		return
	}
	c.reached.Add(-1)
	if result, _ := value.(*Result[R]); result == nil {
		c.settle()
	}
}

// hold makes Publish wait until the matching settle.
func (c *collector[Q, R]) hold() {
	c.waiting.Add(1)
}

// settle counts a subscriber Publish no longer waits for.
func (c *collector[Q, R]) settle() {
	if c.waiting.Add(-1) == 0 {
		select {
		case c.answered <- struct{}{}:
		default:
		}
	}
}

// wait returns each result once all are in or ctx is done, with a *subscriber.TimeoutError about req for the missing
// ones. It is called once Publish has reached every subscriber, so waiting stays at 0 once there.
func (c *collector[Q, R]) wait(ctx context.Context, req Request[Q, R]) map[uint64]Result[R] {
	// Counted first, so that every answer in is returned even once ctx is done.
	for c.waiting.Load() > 0 && ctx.Err() == nil {
		select {
		case <-c.answered:
		case <-ctx.Done():
		}
	}

	// A fresh map, so that a later answer never changes it.
	results := make(map[uint64]Result[R], c.reached.Load())
	c.results.Range(func(key, value any) bool {
		// So that a request a store or a holder keeps retains no result, and a later answer fails its CompareAndSwap.
		c.results.Delete(key)
		id, _ := key.(uint64)
		if result, _ := value.(*Result[R]); result != nil {
			results[id] = *result

			return true
		}
		results[id] = Result[R]{Err: &subscriber.TimeoutError[Request[Q, R]]{SubscriberID: id, Message: req, Err: ctx.Err()}}

		return true
	})

	return results
}

// answering returns options with what makes a subscriber answer: first and attributeBatch outside options' middlewares,
// to see what those return, and a last filter, so that Publish waits only for a subscriber that keeps the request.
func answering[Q, R any](
	first subscriber.Middleware[Request[Q, R]], options []subscriber.Option[Request[Q, R]],
) []subscriber.Option[Request[Q, R]] {
	all := make([]subscriber.Option[Request[Q, R]], 0, len(options)+3)
	all = append(all, subscriber.WithMiddleware(first), subscriber.WithBatchMiddleware(attributeBatch[Q, R]))
	all = append(all, options...)

	return append(all, subscriber.WithFilter(reach[Q, R]))
}

// reach is the last filter of a subscriber that answers.
func reach[Q, R any](req Request[Q, R]) bool {
	if req.to != nil {
		req.to.reach()
	}

	return true
}

// attribute returns handle's err, as a *subscriber.HandleError unless the library made it about id and req.
func attribute[Q, R any](id uint64, req Request[Q, R], err error) error {
	if of, about, _, ok := whose[Q, R](err); ok && of == id && about.to == req.to {
		return err
	}

	return &subscriber.HandleError[Request[Q, R]]{SubscriberID: id, Message: req, Err: err}
}

// attributeBatch is attribute for a batch handler, as a batch middleware.
func attributeBatch[Q, R any](next subscriber.Handler[message.Batch[Request[Q, R]]]) subscriber.Handler[message.Batch[Request[Q, R]]] {
	return func(ctx context.Context, id uint64, batch message.Batch[Request[Q, R]]) error {
		err := next(ctx, id, batch)
		if err == nil {
			return nil
		}
		if of, _, about, ok := whose[Q, R](err); ok && of == id && about != nil {
			return err
		}

		return &subscriber.HandleError[message.Batch[Request[Q, R]]]{SubscriberID: id, Message: batch, Err: err}
	}
}

// fail is Publish's error handler: it answers err for the subscriber and the request or batch err is about, if the
// library made it.
func fail[Q, R any](err error) {
	id, req, batch, ok := whose[Q, R](err)
	if !ok {
		return
	}
	result := Result[R]{Err: err}
	if batch == nil {
		req.answer(id, result)

		return
	}
	for req := range batch.All() {
		req.answer(id, result)
	}
}

// whose returns the subscriber, and the request or batch, that the first error the library made in err's chain is
// about. A *subscriber.StoreError, which wraps two, comes first when there is one.
func whose[Q, R any](err error) (uint64, Request[Q, R], message.Batch[Request[Q, R]], bool) {
	for ; err != nil; err = errors.Unwrap(err) {
		//nolint:errorlint // walks the chain itself, since errors.As takes one type
		switch e := err.(type) {
		case *subscriber.HandleError[Request[Q, R]]:
			return e.SubscriberID, e.Message, nil, true
		case *subscriber.PanicError[Request[Q, R]]:
			return e.SubscriberID, e.Message, nil, true
		case *subscriber.ExpiredError[Request[Q, R]]:
			return e.SubscriberID, e.Message, nil, true
		case *subscriber.TimeoutError[Request[Q, R]]:
			return e.SubscriberID, e.Message, e.Batch, true
		case *subscriber.DroppedError[Request[Q, R]]:
			return e.SubscriberID, e.Message, e.Batch, true
		case *subscriber.ClosedError[Request[Q, R]]:
			return e.SubscriberID, e.Message, e.Batch, true
		case *subscriber.EvictedError[Request[Q, R]]:
			return e.SubscriberID, e.Message, e.Batch, true
		case *subscriber.StoreError[Request[Q, R]]:
			return e.SubscriberID, e.Message, e.Batch, true
		// From a batch handler or its middlewares, about the batch.
		case *subscriber.HandleError[message.Batch[Request[Q, R]]]:
			return e.SubscriberID, Request[Q, R]{}, e.Message, true
		case *subscriber.PanicError[message.Batch[Request[Q, R]]]:
			return e.SubscriberID, Request[Q, R]{}, e.Message, true
		case *subscriber.ExpiredError[message.Batch[Request[Q, R]]]:
			return e.SubscriberID, Request[Q, R]{}, e.Message, true
		}
	}

	return 0, Request[Q, R]{}, nil, false
}
