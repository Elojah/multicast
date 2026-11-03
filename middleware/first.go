package middleware

import (
	"context"
	"slices"
	"sync"

	"github.com/elojah/multicast/subscriber"
)

// Race is what the subscribers FirstN wraps race for: one per message, which carries it. Create one with NewRace.
type Race struct {
	// done is done once the race has its winners or is stopped.
	done context.Context //nolint:containedctx // the race's, which outlives every handle
	stop context.CancelFunc

	mu      sync.Mutex
	winners []uint64
}

// NewRace returns a Race no subscriber has won yet.
func NewRace() *Race {
	done, stop := context.WithCancel(context.Background())

	return &Race{done: done, stop: stop}
}

// Done returns a channel closed once FirstN's n subscribers have won, or once Stop is called. It stays open until
// then.
func (r *Race) Done() <-chan struct{} {
	return r.done.Done()
}

// Stop ends the race, keeping the winners it has: the subscribers still handling the message have their ctx cancelled,
// and those yet to handle it skip it, as once it has its winners.
func (r *Race) Stop() {
	r.stop()
}

// Winners returns the IDs of the subscribers that have won, in the order they did.
func (r *Race) Winners() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.winners)
}

// win makes id a winner, unless the race is done, and ends it once n have won.
func (r *Race) win(id uint64, n int) {
	r.mu.Lock()
	if r.done.Err() != nil || len(r.winners) >= n {
		r.mu.Unlock()

		return
	}
	r.winners = append(r.winners, id)
	full := len(r.winners) == n
	r.mu.Unlock()

	if full {
		r.stop()
	}
}

// FirstN makes the subscribers race for each message race returns a Race for: the first n whose wrapped handler returns
// nil win, the n-th cancelling the ctx of every other one still handling the message, and making one yet to handle it
// skip it. A loser returns nil whatever the wrapped handler returns, since the message was handled: its error is
// neither reported nor stored, and Stats count it as handled. A message without a Race is handled as usual. n less
// than 1 means 1.
//
// Give it to each subscriber, right after Recover: History then records only the winners, and Retry stops waiting once
// the race is over. Every subscriber racing for a message needs the same n, so share one: it keeps no state. With
// SubscribeSeq, the loop's body wins once it returns without calling fail.
func FirstN[T any](n int, race func(msg T) *Race) subscriber.Middleware[T] {
	n = max(n, 1)

	return func(next subscriber.Handler[T]) subscriber.Handler[T] {
		return func(ctx context.Context, id uint64, msg T) error {
			r := race(msg)
			if r == nil {
				return next(ctx, id, msg)
			}
			if r.done.Err() != nil {
				return nil
			}

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			stop := context.AfterFunc(r.done, cancel) //nolint:contextcheck // only watches the race, ctx is the handler's
			defer stop()

			err := next(ctx, id, msg)
			if err == nil {
				r.win(id, n)

				return nil
			}
			// A loser, since it has not won.
			if r.done.Err() != nil {
				return nil
			}

			return err
		}
	}
}
