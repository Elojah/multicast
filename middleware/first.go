package middleware

import (
	"context"
	"slices"
	"sync"

	"github.com/elojah/multicast/subscriber"
)

// Race is what the subscribers FirstN wraps race for: one per message, which carries it. Create one with NewRace.
type Race struct {
	// done is closed once the race has its winners or is stopped.
	done chan struct{}

	mu      sync.Mutex
	winners []uint64
	// cancels holds a cancel for each handler that joined, until the race ends: one that has returned stays, since calling
	// its cancel again does nothing.
	cancels []context.CancelFunc
}

// NewRace returns a Race no subscriber has won yet.
func NewRace() *Race {
	return &Race{done: make(chan struct{})}
}

// Done returns a channel closed once FirstN's n subscribers have won, or once Stop is called. It stays open until
// then, so for good if fewer than n win: every subscriber failing, or too few of them.
func (r *Race) Done() <-chan struct{} {
	return r.done
}

// Stop ends the race, keeping the winners it has: the subscribers still handling the message have their ctx cancelled,
// and those yet to handle it skip it, as once it has its winners. Deferred after Publish, it also ends a Retry inside
// FirstN once the caller gives up.
func (r *Race) Stop() {
	r.mu.Lock()
	if !r.over() {
		close(r.done)
	}
	cancels := r.cancels
	r.cancels = nil
	r.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

// Winners returns the IDs of the subscribers that have won, in the order they did.
func (r *Race) Winners() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.winners)
}

// Won reports whether id has won.
func (r *Race) Won(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Contains(r.winners, id)
}

// over reports whether done is closed.
func (r *Race) over() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// join makes the race call cancel once it ends, and reports whether it is still on, which it may no longer be though
// over said so.
func (r *Race) join(cancel context.CancelFunc) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.over() {
		return false
	}
	r.cancels = append(r.cancels, cancel)

	return true
}

// win makes id a winner, unless the race is over or full, and ends it once n have won.
func (r *Race) win(id uint64, n int) {
	r.mu.Lock()
	if r.over() || len(r.winners) >= n {
		r.mu.Unlock()

		return
	}
	r.winners = append(r.winners, id)
	full := len(r.winners) == n
	r.mu.Unlock()

	if full {
		r.Stop()
	}
}

// FirstN makes the subscribers race for each message race returns a Race for: the first n whose wrapped handler returns
// nil win, the n-th cancelling the ctx of every other one still handling the message, and making one yet to handle it
// skip it. A loser returns nil whatever the wrapped handler returns, since the message was handled: its error is
// neither reported nor stored, and Stats count it as handled. A subscriber failing while the race is on has not lost:
// its error is reported as usual, even if another wins later. A message without a Race is handled as usual. n less
// than 1 means 1.
//
// Give it to each subscriber, right after Recover: History then records no loser that has not handled the message, and
// Retry stops waiting once the race is over. A Retry outside FirstN waits out its delay, as one from
// multicast.WithDefaultSubscriberOptions does around a FirstN given to Subscribe: give both as defaults. Every
// subscriber racing for a message needs the same n, so share one: it keeps no state.
//
// With SubscribeSeq, the loop's body wins once it returns without calling fail. It gets no ctx to be cancelled through,
// so it watches Race.Done. A batch handler does not race. Under a reply.Group, use reply.FirstN.
func FirstN[T any](n int, race func(msg T) *Race) subscriber.Middleware[T] {
	n = max(n, 1)

	return func(next subscriber.Handler[T]) subscriber.Handler[T] {
		return func(ctx context.Context, id uint64, msg T) error {
			r := race(msg)
			if r == nil {
				return next(ctx, id, msg)
			}
			if r.over() {
				return nil
			}

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			if !r.join(cancel) {
				return nil
			}

			err := next(ctx, id, msg)
			if err == nil {
				r.win(id, n)

				return nil
			}
			// A loser, since it has not won.
			if r.over() {
				return nil
			}

			return err
		}
	}
}
