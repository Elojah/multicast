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
// A Group is a multicast.Group of Requests, which are messages like any other, so the Group's options, the subscribers'
// and the Publish's apply as they are. Publish waits until each subscriber the request reached has answered, with its
// value or an error, why the library lost the request included, or until ctx is done. With FirstN, only the first
// subscribers to answer count.
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
	"github.com/elojah/multicast/middleware"
	"github.com/elojah/multicast/subscriber"
)

// Group is a multicast.Group of Requests, whose Subscribe and SubscribeSeq answer them and whose Publish waits for the
// answers. Its other methods are the embedded Group's: PublishBatch, like g.Group.Publish, sends requests whose answers
// are dropped, and Publish never waits for a subscriber added through g.Group. To wrap a Group made elsewhere:
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
// options return nil, and an error once reported, with the zero value, so that a middleware from
// multicast.WithDefaultSubscriberOptions, such as Retry or Recover, answers the attempt that ended. Past options,
// handle's error is a *subscriber.HandleError unless the library made it, so that Publish knows whose it is:
// middleware.WrapError is not needed. So is a batch handler's (subscriber.WithBatchHandler), about the batch, which
// answers each request of the batch not answered yet.
//
// A request stays unanswered if a middleware from WithDefaultSubscriberOptions returns nil on an error, or an error of
// its own that names no subscriber.
func (g *Group[Q, R]) Subscribe(
	ctx context.Context, handle func(ctx context.Context, id uint64, query Q) (R, error),
	options ...subscriber.Option[Request[Q, R]],
) (uint64, error) {
	// last is what handle returned for the request under way. Only the subscriber's goroutine uses it, one request at a
	// time.
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

// SubscribeSeq is multicast.Group.SubscribeSeq for a loop answering each request with Request.Answer, or with fail,
// whose error answers as with Subscribe, and so does a batch handler's.
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
// (subscriber.WithDefaultMessageOptions) does not run. A request a holder merges away is answered only by the one it is
// merged into with Request.Merge, as with package reply/holder's NewLatest. Called from handle, Publish waits for that
// subscriber until ctx is done, since it answers only once handle returns.
//
// A holder makes Publish wait for its window, which serial Publishes from one goroutine never share. middleware.Retry
// waits for handle's ctx, the subscriber's unless message.WithContext passes ctx, so that it stops once Publish
// returns rather than holding the subscriber up.
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

// FirstN is middleware.FirstN for a Group, for each query race returns a Race for: a subscriber that loses leaves the
// Publish, whatever it answered, so that Publish returns the winners, and those that failed before the race ended.
// Unlike middleware.FirstN, it works from multicast.WithDefaultSubscriberOptions and with SubscribeSeq:
//
//	racing := subscriber.WithMiddleware(reply.FirstN[Q, R](1, func(q Q) *middleware.Race { return q.race }))
//	...
//	race := middleware.NewRace()
//	defer race.Stop()
//	results := g.Publish(ctx, Q{race: race})
//
// Publish still waits for each loser to stop, so for one ignoring its ctx or busy with another request, until ctx is
// done. Stopping the race once Publish returns ends a middleware.Retry inside FirstN that would hold up the next one.
// Do not end Publish's ctx on Race.Done, which closes before the last winner answers.
func FirstN[Q, R any](n int, race func(query Q) *middleware.Race) subscriber.Middleware[Request[Q, R]] {
	first := middleware.FirstN(n, func(req Request[Q, R]) *middleware.Race { return race(req.Query) })

	return func(next subscriber.Handler[Request[Q, R]]) subscriber.Handler[Request[Q, R]] {
		racing := first(next)

		return func(ctx context.Context, id uint64, req Request[Q, R]) error {
			r := race(req.Query)
			if r == nil {
				return racing(ctx, id, req)
			}
			// A loser may answer before it loses: Publish waits to know, so that it never returns that answer.
			req.each((*collector[Q, R]).hold)
			defer req.each((*collector[Q, R]).settle)

			err := racing(ctx, id, req)
			if err == nil && !r.Won(id) {
				req.each(func(c *collector[Q, R]) { c.withdraw(id) })
			}

			return err
		}
	}
}

// Request is a message carrying Query, which subscribers answer. Only Group.Publish sets where the answers go: a
// request published otherwise, replayed or read back from a store has nowhere to send them, so Answer drops them.
type Request[Q, R any] struct {
	Query Q

	// to is the collector of the Publish that sent the request, and merged the requests merged into it.
	to     *collector[Q, R]
	merged []Request[Q, R]
}

// Answer sends id's answer to the Publish that sent r, and to those of the requests merged into it, for a SubscribeSeq
// loop or a batch handler: Group.Subscribe's handle answers on its own. It never blocks. Only a subscriber's first answer
// counts, and one coming after Publish has returned is dropped.
func (r Request[Q, R]) Answer(id uint64, value R, err error) {
	r.answer(id, Result[R]{Value: value, Err: err})
}

// Merge returns r, also answering for older, for a holder that merges requests away, such as package reply/holder's,
// which otherwise leaves them unanswered. The request returned keeps older, its Query included.
func (r Request[Q, R]) Merge(older Request[Q, R]) Request[Q, R] {
	// Clipped, so that requests merged from the same r never share an array.
	r.merged = append(slices.Clip(r.merged), older)

	return r
}

func (r Request[Q, R]) answer(id uint64, result Result[R]) {
	if r.to != nil {
		r.to.answer(id, result)
	}
	for _, older := range r.merged {
		older.answer(id, result)
	}
}

// each calls f with the collector of the Publish that sent r, and those of the requests merged into it.
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
	// reached counts the subscribers reached, and waiting those that have not answered, and those holding Publish.
	reached atomic.Int64
	waiting atomic.Int64
	// answered wakes wait up once waiting reaches 0. Buffered, so that settle never waits.
	answered chan struct{}

	// at is the subscriber Publish is at, set by its visit (message.WithVisit) for reach, which the subscriber's own
	// filters call right after, in Publish's goroutine.
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

// withdraw makes Publish neither wait for id nor return it, even if it has answered, and fails any answer after. It
// never blocks.
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

// hold makes Publish wait until the matching settle, for a subscriber it still waits for.
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
		// So that a request a store or a holder keeps retains no result, and that a later answer fails its
		// CompareAndSwap.
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

// answering returns options with what makes a subscriber answer: first and attributeBatch, outside the middlewares and
// batch middlewares in options so that they see what those return, and a filter after the others, so that Publish waits
// only for a subscriber that keeps the request.
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

// attributeBatch is the batch middleware of a subscriber that answers: it returns the batch handler's error as a
// *subscriber.HandleError about the batch, unless the library made it about the subscriber and a batch.
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

// fail is Publish's error handler: it answers err for the subscriber and the request, or each of the batch, err is
// about, if the library made it.
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
// about, a batch handler's included. A *subscriber.StoreError, which wraps two, comes first when there is one.
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
