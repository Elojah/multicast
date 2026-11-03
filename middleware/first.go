package middleware

import (
	"context"
	"slices"
	"sync"

	"github.com/elojah/multicast/subscriber"
)

// Race is what subscribers under FirstN race for: one per message, which carries it. Create one with NewRace.
type Race struct {
	// done is closed once the race is over.
	done chan struct{}

	mu      sync.Mutex
	winners []uint64
	// cancels holds each joined handler's cancel until the race ends. Calling one that has returned does nothing.
	cancels []context.CancelFunc
}

// NewRace returns a Race no subscriber has won yet.
func NewRace() *Race {
	return &Race{done: make(chan struct{})}
}

// Done returns a channel closed once n subscribers have won or Stop is called, so never if fewer than n win.
func (r *Race) Done() <-chan struct{} {
	return r.done
}

// Stop ends the race, keeping its winners: subscribers still handling the message have their ctx cancelled, and those
// yet to handle it skip it. Deferred after Publish, it also ends a Retry inside FirstN once the caller gives up.
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

// Winners returns the IDs of the winners, in the order they won.
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

// join makes the race call cancel once it ends, or reports false if it already has.
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

// FirstN makes subscribers race for each message race returns a Race for: the first n whose wrapped handler returns nil
// win, and the n-th ends the race, as Race.Stop does. A message without a Race is handled as usual. n less than 1 means
// 1.
//
// A loser returns nil whatever the wrapped handler returns: its error is neither reported nor stored, and Stats count
// the message as handled. A subscriber failing while the race is on has not lost: its error is reported as usual, even
// if another wins later.
//
// Share one FirstN between the subscribers, which need the same n: it keeps no state. A Retry outside it waits out its
// delay, as a default one (multicast.WithDefaultSubscriberOptions) does around a FirstN given to Subscribe: give both
// as defaults.
//
// A SubscribeSeq loop's body wins once it returns without calling fail. It has no ctx, so it watches Race.Done. A batch
// handler does not race. Under a reply.Group, use package reply/middleware's FirstN.
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
