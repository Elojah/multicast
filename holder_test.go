package multicast_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast/holder"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/subscriber"
)

const holdFor = time.Second

// byTens returns a holder.Reduce keeping the latest message of each ten (11, 12 and 13 share one) for d.
func byTens(d time.Duration) *holder.Reduce[int] {
	return holder.NewReduce(d, func(held []int, next int) []int {
		for i, msg := range held {
			if msg/10 == next/10 {
				held[i] = next

				return held
			}
		}

		return append(held, next)
	})
}

// The subscriber holds each message it takes, and handles, through its middlewares, what the holder releases once
// due. Stats count the messages held and merged.
func TestSubscriberWithHolder(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		r := &recorder[int]{}
		wrapped := &recorder[int]{}
		id := subscribe(t, g, r.handle, subscriber.WithHolder(byTens(holdFor)),
			subscriber.WithMiddleware(func(next subscriber.Handler[int]) subscriber.Handler[int] {
				return func(ctx context.Context, id uint64, msg int) error {
					wrapped.record(msg)

					return next(ctx, id, msg)
				}
			}))

		for _, msg := range []int{11, 21, 12, 13} {
			g.Publish(t.Context(), msg)
		}
		synctest.Wait()
		if got := r.messages(); len(got) != 0 {
			t.Errorf("handle got %v within the window, want nothing yet", got)
		}
		checkStats(t, g, id, subscriber.Stats{Delivered: 4, Held: 2, Merged: 2})

		time.Sleep(holdFor)
		synctest.Wait()
		want := []int{13, 21}
		if got := r.messages(); !slices.Equal(got, want) {
			t.Errorf("handle got %v once the window ended, want %v", got, want)
		}
		if got := wrapped.messages(); !slices.Equal(got, want) {
			t.Errorf("the middleware got %v, want %v", got, want)
		}
		checkStats(t, g, id, subscriber.Stats{Delivered: 4, Handled: 2, Merged: 2})
	})
}

// Shutdown releases everything held at once, and waits for handle, then done.
func TestSubscriberWithHolder_Shutdown(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		r := &recorder[int]{}
		done := make(chan uint64, 1)
		id := subscribe(t, g, r.handle, subscriber.WithHolder(byTens(time.Hour)), subscriber.WithDone[int](t.Context(), done))
		for _, msg := range []int{11, 21, 12} {
			g.Publish(t.Context(), msg)
		}

		start := time.Now()
		if err := waitShutdown(t, shutdown(t.Context(), g)); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Shutdown took %v, want no wait for the window", elapsed)
		}
		if got, want := r.messages(), []int{12, 21}; !slices.Equal(got, want) {
			t.Errorf("handle got %v once Shutdown returned, want %v", got, want)
		}
		if got := received(done); !slices.Equal(got, []uint64{id}) {
			t.Errorf("done got %v once Shutdown returned, want %v", got, id)
		}
	})
}

// Once unsubscribed with WithUnsubscribeDiscard, what the holder holds is reported as *subscriber.ClosedError.
func TestSubscriberWithHolder_Discard(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		r := &recorder[int]{}
		errs := &recorder[error]{}
		id := subscribe(t, g, r.handle, subscriber.WithHolder(byTens(time.Hour)),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) { errs.record(err) }))
		for _, msg := range []int{11, 21, 12} {
			g.Publish(t.Context(), msg)
		}
		// Publish returns once 12 is taken, maybe before it is held: discarding then would report it, and 11 unmerged.
		synctest.Wait()

		if err := g.Unsubscribe(t.Context(), id, subscriber.WithUnsubscribeDiscard()); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		synctest.Wait()
		if got := r.messages(); len(got) != 0 {
			t.Errorf("handle got %v, want nothing", got)
		}
		got := describeLosses(t, errs.messages())
		if want := []string{"12 closed", "21 closed"}; !slices.Equal(got, want) {
			t.Errorf("error handler got %v, want %v", got, want)
		}
	})
}

// subscriber.WithEvict sees a holder's *subscriber.DroppedError, and evicts the subscriber, discarding what it holds.
func TestSubscriberWithHolder_Evict(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		r := &recorder[int]{}
		errs := &recorder[error]{}
		subscribe(t, g, r.handle, subscriber.WithHolder[int](&capped{n: 1, d: time.Hour}),
			subscriber.WithEvict[int](func(err error) bool { return errors.Is(err, subscriber.ErrDropped) }),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) { errs.record(err) }))
		g.Publish(t.Context(), 11)
		g.Publish(t.Context(), 21)
		synctest.Wait()

		if got := r.messages(); len(got) != 0 {
			t.Errorf("handle got %v, want nothing", got)
		}
		got := describeLosses(t, errs.messages())
		if want := []string{"21 evicted, dropped", "11 closed"}; !slices.Equal(got, want) {
			t.Errorf("error handler got %v, want %v", got, want)
		}
		if stats := g.Stats(); len(stats) != 0 {
			t.Errorf("Stats once evicted are for %d subscribers, want none", len(stats))
		}
	})
}

// describeLosses describes each error with describeLoss.
func describeLosses(t *testing.T, errs []error) []string {
	t.Helper()
	got := make([]string, 0, len(errs))
	for _, err := range errs {
		got = append(got, describeLoss(t, err))
	}

	return got
}

// capped is a holder keeping each message for d, merging none, with room for n.
type capped struct {
	n    int
	d    time.Duration
	held []message.Message[int]
	due  []time.Time
}

func (c *capped) Hold(m message.Message[int], now time.Time) (int, bool) {
	if len(c.held) == c.n {
		return 0, false
	}
	c.held, c.due = append(c.held, m), append(c.due, now.Add(c.d))

	return 0, true
}

func (c *capped) Release(now time.Time, all bool) (message.Message[int], time.Duration, bool) {
	if len(c.held) == 0 {
		return message.Message[int]{}, 0, false
	}
	if wait := c.due[0].Sub(now); wait > 0 && !all {
		return message.Message[int]{}, wait, false
	}
	m := c.held[0]
	c.held, c.due = c.held[1:], c.due[1:]

	return m, 0, true
}
