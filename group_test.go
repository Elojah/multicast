package multicast_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast"
	"github.com/elojah/multicast/filter"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/subscriber"
)

// deadlockTimeout bounds every wait, so a deadlock fails instead of hanging. In a synctest bubble, it fires exactly
// when the bubble deadlocks.
const deadlockTimeout = 10 * time.Second

func TestPublish_DeliversAllMessagesInOrder(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const subscribers = 3
		g := newGroup[int](t)
		ids := make([]uint64, 0, subscribers)
		recorders := make([]*recorder[int], 0, subscribers)
		for range subscribers {
			r, id := record(t, g)
			ids = append(ids, id)
			recorders = append(recorders, r)
		}

		for msg := 1; msg <= 4; msg++ {
			g.Publish(t.Context(), msg)
		}
		unsubscribeAll(t, g, ids...)
		synctest.Wait()

		for i, r := range recorders {
			if got, want := r.messages(), []int{1, 2, 3, 4}; !slices.Equal(got, want) {
				t.Errorf("subscriber %d received %v, want %v", i, got, want)
			}
		}
	})
}

// A Group never closed leaves no goroutine once its subscribers are done, so it can be garbage collected.
func TestNew_NotClosed(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := multicast.New[int]()
		id := subscribe(t, g, func(context.Context, uint64, int) error { return nil })
		g.Publish(t.Context(), 1)
		unsubscribe(t, g, id)
		// synctest.Test fails if a goroutine is left.
	})
}

// The error handler gets every error handle returns, as is and in order, and both get the Subscribe ctx.
func TestSubscriberWithErrorHandler(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		ctx := context.WithValue(subscribeCtx(t), key{}, "subscribe")
		checkCtx := func(ctx context.Context, what string) {
			if v := ctx.Value(key{}); v != "subscribe" {
				t.Errorf("%s got a ctx with value %v, want the one passed to Subscribe", what, v)
			}
		}
		g := newGroup[int](t)
		errs := &recorder[error]{}
		id, err := g.Subscribe(ctx, func(ctx context.Context, _ uint64, msg int) error {
			checkCtx(ctx, "handle")
			if msg%2 == 1 {
				return handleError(msg)
			}

			return nil
		}, subscriber.WithErrorHandler[int](func(ctx context.Context, err error) {
			checkCtx(ctx, "the error handler")
			errs.record(err)
		}))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		for msg := 1; msg <= 4; msg++ {
			g.Publish(t.Context(), msg)
		}
		unsubscribe(t, g, id)
		synctest.Wait()

		if got, want := errs.messages(), []error{handleError(1), handleError(3)}; !slices.Equal(got, want) {
			t.Errorf("error handler got %v, want %v", got, want)
		}
	})
}

// Without an error handler, handle's errors are discarded without holding the subscriber up.
func TestSubscribe_ErrorsWithoutHandler(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		r := &recorder[int]{}
		id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
			r.record(msg)

			return handleError(msg)
		})

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		for msg := range 3 {
			g.Publish(ctx, msg)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Publish calls returned after %v, want no wait", elapsed)
		}

		unsubscribe(t, g, id)
		synctest.Wait()
		if got, want := r.messages(), []int{0, 1, 2}; !slices.Equal(got, want) {
			t.Errorf("subscriber processed %v, want %v", got, want)
		}
	})
}

// IDs count up from 1 in each Group, SubscribeSeq's included, and a subscription refused after Close gets 0.
func TestSubscribe_IDs(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		handle := func(context.Context, uint64, int) error { return nil }
		for range 2 {
			g := newGroup[int](t)
			first := subscribe(t, g, handle)
			second, seq := subscribeSeq(t, g)
			third := subscribe(t, g, handle)
			if got, want := []uint64{first, second, third}, []uint64{1, 2, 3}; !slices.Equal(got, want) {
				t.Errorf("IDs %v, want %v", got, want)
			}

			if err := g.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			for msg := range seq {
				t.Errorf("loop got %d, want nothing", msg)
			}
			if id, err := g.Subscribe(subscribeCtx(t), handle); id != 0 || !errors.Is(err, multicast.ErrClosed) {
				t.Errorf("Subscribe after Close = %d, %v, want 0, ErrClosed", id, err)
			}
		}
		synctest.Wait()
	})
}

func TestUnsubscribe_Twice(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		r, id := record(t, g)
		unsubscribe(t, g, id)

		// A repeated Unsubscribe fails without dropping the reference again, before a Publish...
		if err := g.Unsubscribe(t.Context(), id); !isNotFound(err) {
			t.Errorf("second Unsubscribe = %v, want *SubscriberNotFoundError", err)
		}
		if p := publish(t.Context(), g, 1); p != nil {
			t.Fatalf("Publish panicked: %v", p)
		}

		// ...or after one.
		if err := g.Unsubscribe(t.Context(), id); !isNotFound(err) {
			t.Errorf("third Unsubscribe = %v, want *SubscriberNotFoundError", err)
		}
		if p := publish(t.Context(), g, 2); p != nil {
			t.Errorf("Publish after a repeated Unsubscribe panicked: %v", p)
		}

		synctest.Wait()
		if got := r.messages(); len(got) != 0 {
			t.Errorf("unsubscribed subscriber received %v, want nothing", got)
		}
	})
}

// However a stuck subscriber is removed, and whatever the delivery, a Publish waiting on it with a ctx never done is
// freed at once: the message is reported as a *subscriber.ClosedError, and what the subscriber took is still handled.
func TestUnsubscribe_FreesPublish(t *testing.T) {
	t.Parallel()

	removals := []struct {
		name   string
		remove func(t *testing.T, g *multicast.Group[int], id uint64, cancel context.CancelFunc)
	}{
		{"Unsubscribe", func(t *testing.T, g *multicast.Group[int], id uint64, _ context.CancelFunc) {
			t.Helper()
			unsubscribe(t, g, id)
		}},
		{"Close", func(t *testing.T, g *multicast.Group[int], _ uint64, _ context.CancelFunc) {
			t.Helper()
			if err := g.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}},
		{"ContextDone", func(t *testing.T, _ *multicast.Group[int], _ uint64, cancel context.CancelFunc) {
			t.Helper()
			cancel()
		}},
	}
	deliveries := []struct {
		name   string
		option message.Option[int]
	}{
		{"Sync", message.WithSync[int]()},
		{"Parallel", message.WithParallel[int]()},
		{"Async", message.WithAsync[int]()},
	}
	for _, removal := range removals {
		for _, delivery := range deliveries {
			t.Run(removal.name+"/"+delivery.name, func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(subscribeCtx(t))
					defer cancel()
					g := newGroup[int](t)
					handled := &recorder[int]{hold: make(chan struct{})}
					closed := &recorder[int]{}
					id, err := g.Subscribe(ctx, handled.handle, subscriber.WithBuffer[int](1),
						subscriber.WithErrorHandler[int](recordClosed(t, closed)))
					if err != nil {
						t.Fatalf("Subscribe: %v", err)
					}

					// handle holds on to 0, 1 fills the buffer, and the Publish of 2 waits.
					g.Publish(t.Context(), 0)
					g.Publish(t.Context(), 1)
					done := make(chan struct{})
					go func() {
						defer close(done)
						g.Publish(t.Context(), 2, delivery.option)
					}()
					synctest.Wait()

					removal.remove(t, g, id, cancel)
					synctest.Wait()
					select {
					case <-done:
					default:
						t.Errorf("Publish still waits on its subscriber once removed")
					}
					if got, want := closed.messages(), []int{2}; !slices.Equal(got, want) {
						t.Errorf("error handler got *subscriber.ClosedError for messages %v, want %v", got, want)
					}

					handled.release()
					synctest.Wait()
					if got, want := handled.messages(), []int{0, 1}; !slices.Equal(got, want) {
						t.Errorf("handle got %v, want %v", got, want)
					}
				})
			})
		}
	}
}

// handle unsubscribes itself while a Publish waits on it: Unsubscribe must not wait, and the Publish gives up.
func TestUnsubscribe_SelfDuringPublish(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		g := newGroup[int](t)
		other, otherID := record(t, g)

		self := &recorder[int]{}
		proceed := make(chan struct{})
		subscribe(t, g, func(ctx context.Context, selfID uint64, msg int) error {
			self.record(msg)
			if msg == 1 {
				<-proceed
				if err := g.Unsubscribe(ctx, selfID); err != nil {
					t.Errorf("Unsubscribe: %v", err)
				}
			}

			return nil
		})

		g.Publish(ctx, 1) // self is now processing 1 and not reading
		done := make(chan struct{})
		go func() {
			defer close(done)
			g.Publish(ctx, 2)
		}()
		synctest.Wait() // the Publish of 2 is waiting on self
		close(proceed)
		waitClosed(t, done, "Publish during which the subscriber unsubscribed itself")

		g.Publish(ctx, 3)
		unsubscribeAll(t, g, otherID)
		synctest.Wait()

		if got, want := self.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("unsubscribed subscriber received %v, want %v", got, want)
		}
		if got, want := other.messages(), []int{1, 2, 3}; !slices.Equal(got, want) {
			t.Errorf("other subscriber received %v, want %v", got, want)
		}
	})
}

// Publish gets to subscribers in subscription order, and skips without a report one unsubscribed since it started.
func TestPublish_SubscriptionOrder(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const subscribers = 20
		g := newGroup[int](t)
		errs := &recorder[error]{}
		report := subscriber.WithErrorHandler[int](func(_ context.Context, err error) { errs.record(err) })
		ignore := func(context.Context, uint64, int) error { return nil }

		// The first subscriber's filter, which Publish calls in its goroutine, unsubscribes the second.
		var second uint64
		ids := make([]uint64, 0, subscribers)
		ids = append(ids, subscribe(t, g, ignore, report, subscriber.WithFilter(func(int) bool {
			unsubscribe(t, g, second)

			return true
		})))
		for range subscribers - 1 {
			ids = append(ids, subscribe(t, g, ignore, report))
		}
		second = ids[1]

		var visited []uint64
		g.Publish(t.Context(), 1, message.WithVisit[int](func(id uint64) { visited = append(visited, id) }))
		ids = slices.Delete(ids, 1, 2)
		unsubscribeAll(t, g, ids...)
		synctest.Wait()

		if !slices.Equal(visited, ids) {
			t.Errorf("Publish visited %v, want %v", visited, ids)
		}
		if got := errs.messages(); len(got) > 0 {
			t.Errorf("errors reported: %v, want none", got)
		}
	})
}

// Publish waits for a stuck buffered subscriber only once the buffer is full, and what is buffered at Unsubscribe is
// still handled.
func TestSubscriberWithBuffer(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const buffer = 3
		g := newGroup[int](t)
		stuck := &recorder[int]{hold: make(chan struct{})}
		id := subscribe(t, g, stuck.handle, subscriber.WithBuffer[int](buffer))
		g.Publish(t.Context(), 0)
		synctest.Wait() // stuck is now processing 0 and not reading

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		for msg := 1; msg <= buffer; msg++ {
			g.Publish(ctx, msg)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Publish calls that fit in the buffer returned after %v, want no wait", elapsed)
		}
		g.Publish(ctx, buffer+1)
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("Publish to a full buffer returned after %v, want it to give up at the %v deadline",
				elapsed, time.Second)
		}

		unsubscribe(t, g, id)
		stuck.release()
		synctest.Wait()
		if got, want := stuck.messages(), []int{0, 1, 2, 3}; !slices.Equal(got, want) {
			t.Errorf("buffered subscriber received %v, want %v", got, want)
		}
	})
}

// An async Publish never waits, and its send to a stuck subscriber gives up with a *subscriber.ClosedError once that
// one is unsubscribed.
func TestMessageWithAsync(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		stuck := &recorder[int]{hold: make(chan struct{})}
		closed := &recorder[int]{}
		stuckID := subscribe(t, g, stuck.handle, subscriber.WithErrorHandler[int](recordClosed(t, closed)))
		reader, readerID := record(t, g)
		g.Publish(t.Context(), 0) // stuck is now processing 0 and not reading

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		g.Publish(ctx, 1, message.WithAsync[int]())
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("async Publish returned after %v, want no wait", elapsed)
		}
		synctest.Wait()
		if got, want := reader.messages(), []int{0, 1}; !slices.Equal(got, want) {
			t.Errorf("reader received %v while stuck was still processing, want %v", got, want)
		}

		unsubscribeAll(t, g, stuckID, readerID)
		synctest.Wait()
		if got, want := closed.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("stuck subscriber's error handler got *subscriber.ClosedError for %v once unsubscribed, want %v", got, want)
		}

		stuck.release()
		synctest.Wait()
		if got, want := stuck.messages(), []int{0}; !slices.Equal(got, want) {
			t.Errorf("stuck subscriber received %v, want %v", got, want)
		}
	})
}

// Past subscriber.WithAsyncLimit sends under way, an async message is dropped at once and not counted, until the
// subscriber takes them.
func TestSubscriberWithAsyncLimit(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		stuck := &recorder[int]{hold: make(chan struct{})}
		losses := &recorder[string]{}
		id := subscribe(t, g, stuck.handle, subscriber.WithAsyncLimit[int](2),
			subscriber.WithErrorHandler[int](recordLosses(t, losses)))

		g.Publish(t.Context(), 1) // stuck holds on to 1, so 2 and 3 wait for it, and 4 is dropped
		taken := make([]int, 0, 3)
		for msg := 2; msg <= 4; msg++ {
			taken = append(taken, g.Publish(t.Context(), msg, message.WithAsync[int]()))
		}
		if want := []int{1, 1, 0}; !slices.Equal(taken, want) {
			t.Errorf("async Publish calls of 2 to 4 handed them to %v subscribers, want %v", taken, want)
		}
		synctest.Wait()
		if got, want := losses.messages(), []string{"4 dropped"}; !slices.Equal(got, want) {
			t.Errorf("error handler got %q, want %q", got, want)
		}
		checkStats(t, g, id, subscriber.Stats{Sending: 2, Delivered: 1, Dropped: 1})

		stuck.release()
		synctest.Wait()
		checkStats(t, g, id, subscriber.Stats{Delivered: 3, Handled: 3, Dropped: 1})
		if n := g.Publish(t.Context(), 5, message.WithAsync[int]()); n != 1 {
			t.Errorf("async Publish once the sends were taken handed 5 to %d subscribers, want 1", n)
		}
		synctest.Wait()
		unsubscribe(t, g, id)
		synctest.Wait()

		// 2 and 3 in any order.
		got := stuck.messages()
		slices.Sort(got)
		if want := []int{1, 2, 3, 5}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
	})
}

// A message's error handler gets only that message's errors, with the Subscribe ctx, and the subscriber's own still
// gets them.
func TestMessageWithErrorHandler(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		ctx := context.WithValue(subscribeCtx(t), key{}, "subscribe")
		g := newGroup[int](t)
		subscriberErrs := &recorder[int]{}
		failingID, err := g.Subscribe(ctx, func(_ context.Context, _ uint64, msg int) error {
			return handleError(msg)
		}, subscriber.WithErrorHandler[int](recordFailures(t, subscriberErrs)))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		_, okID := record(t, g)

		messageErrs := &recorder[error]{}
		g.Publish(t.Context(), 1, message.WithErrorHandler[int](func(ctx context.Context, err error) {
			if v := ctx.Value(key{}); v != "subscribe" {
				t.Errorf("message error handler got a ctx with value %v, want the one passed to Subscribe", v)
			}
			messageErrs.record(err)
		}))
		g.Publish(t.Context(), 2)
		unsubscribeAll(t, g, failingID, okID)
		synctest.Wait()

		if got, want := messageErrs.messages(), []error{handleError(1)}; !slices.Equal(got, want) {
			t.Errorf("message error handler got %v, want %v", got, want)
		}
		if got, want := subscriberErrs.messages(), []int{1, 2}; !slices.Equal(got, want) {
			t.Errorf("subscriber error handler got errors for messages %v, want %v", got, want)
		}
	})
}

// A message's timeout bounds the wait for each subscriber separately, unlike a ctx deadline.
func TestMessageWithTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		first, firstID := hold(t, g)
		second, secondID := hold(t, g)
		g.Publish(t.Context(), 0) // both are now processing 0 and not reading

		timeouts := &recorder[int]{}
		recordTimeout := recordTimeouts(t, timeouts)
		start := time.Now()
		g.Publish(t.Context(), 1, message.WithTimeout[int](time.Second),
			message.WithErrorHandler[int](func(ctx context.Context, err error) {
				if ctx.Err() != nil {
					t.Errorf("error handler got a ctx that is done (%v), want the Publish's", ctx.Err())
				}
				recordTimeout(ctx, err)
			}))
		if elapsed := time.Since(start); elapsed != 2*time.Second {
			t.Errorf("Publish returned after %v, want %v: the whole timeout for each subscriber", elapsed, 2*time.Second)
		}
		if got, want := timeouts.messages(), []int{1, 1}; !slices.Equal(got, want) {
			t.Errorf("error handler got timeouts for messages %v, want %v", got, want)
		}

		unsubscribeAll(t, g, firstID, secondID)
		first.release()
		second.release()
		synctest.Wait()
		for i, r := range []*recorder[int]{first, second} {
			if got, want := r.messages(), []int{0}; !slices.Equal(got, want) {
				t.Errorf("stuck subscriber %d received %v, want %v", i, got, want)
			}
		}
	})
}

// A subscriber's timeout bounds every Publish's wait, unless the Publish passes its own: longer, shorter, or 0 for
// none.
func TestSubscriberWithTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		stuck := &recorder[int]{hold: make(chan struct{})}
		timeouts := &recorder[int]{}
		id := subscribe(t, g, stuck.handle, subscriber.WithTimeout[int](time.Second),
			subscriber.WithErrorHandler[int](recordTimeouts(t, timeouts)))
		g.Publish(t.Context(), 0) // stuck is now processing 0 and not reading

		start := time.Now()
		g.Publish(t.Context(), 1)
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("Publish returned after %v, want it to give up at the subscriber's %v timeout",
				elapsed, time.Second)
		}

		start = time.Now()
		g.Publish(t.Context(), 2, message.WithTimeout[int](3*time.Second))
		if elapsed := time.Since(start); elapsed != 3*time.Second {
			t.Errorf("Publish returned after %v, want it to give up at the message's %v timeout",
				elapsed, 3*time.Second)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		start = time.Now()
		g.Publish(ctx, 3, message.WithTimeout[int](0))
		if elapsed := time.Since(start); elapsed != 5*time.Second {
			t.Errorf("Publish with no timeout returned after %v, want it to give up at the %v ctx deadline",
				elapsed, 5*time.Second)
		}

		if got, want := timeouts.messages(), []int{1, 2, 3}; !slices.Equal(got, want) {
			t.Errorf("error handler got timeouts for messages %v, want %v", got, want)
		}

		unsubscribe(t, g, id)
		stuck.release()
		synctest.Wait()
		if got, want := stuck.messages(), []int{0}; !slices.Equal(got, want) {
			t.Errorf("stuck subscriber received %v, want %v", got, want)
		}
	})
}

// A Publish that picks a subscriber up just as it is unsubscribed sends before its channel closes or skips it, never
// after. The window is a few instructions wide, hence many rounds.
func TestUnsubscribe_RacesPublishPickingUpChannel(t *testing.T) {
	t.Parallel()

	const iterations = 20000

	for i := range iterations {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			g := newGroup[int](t)
			r, id := record(t, g)

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				if p := publish(ctx, g, 1); p != nil {
					t.Errorf("iteration %d: Publish panicked: %v", i, p)
				}
			})
			wg.Go(func() {
				<-start
				if err := g.Unsubscribe(ctx, id); err != nil {
					t.Errorf("iteration %d: Unsubscribe(%d): %v", i, id, err)
				}
				if p := publish(ctx, g, 2); p != nil {
					t.Errorf("iteration %d: Publish after Unsubscribe panicked: %v", i, p)
				}
			})
			close(start)
			waitGroup(t, &wg, "Publish racing Unsubscribe")

			synctest.Wait()
			if got := r.messages(); len(got) != 0 && !slices.Equal(got, []int{1}) {
				t.Errorf("iteration %d: subscriber received %v, want [] or [1]", i, got)
			}
		})
	}
}

// handle unsubscribes both of its consumer's subscriptions while a Publish waits on the first: Unsubscribe never
// waits on a Publish, even through a lock. In real time, since a goroutine stuck on a mutex hangs a synctest bubble.
func TestUnsubscribe_SeveralThenDrain(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	g := newGroup[int](t)

	proceed := make(chan struct{})
	handleDone := make(chan struct{})
	secondPublishDone := make(chan struct{})
	var secondID uint64
	subscribe(t, g, func(ctx context.Context, firstID uint64, msg int) error {
		if msg != 1 {
			return nil
		}
		defer close(handleDone)
		<-proceed
		if err := g.Unsubscribe(ctx, firstID); err != nil {
			t.Errorf("Unsubscribe(first): %v", err)
		}
		go func() {
			defer close(secondPublishDone)
			g.Publish(ctx, 3)
		}()
		time.Sleep(10 * time.Millisecond) // let the second Publish get as far as it can
		if err := g.Unsubscribe(ctx, secondID); err != nil {
			t.Errorf("Unsubscribe(second): %v", err)
		}

		return nil
	})
	g.Publish(ctx, 1) // first is now processing 1 and not reading
	_, secondID = record(t, g)

	firstPublishDone := make(chan struct{})
	go func() {
		defer close(firstPublishDone)
		g.Publish(ctx, 2)
	}()
	time.Sleep(10 * time.Millisecond) // let the first Publish get to first and wait on it
	close(proceed)

	waitClosed(t, handleDone, "handle to leave both subscriptions")
	waitClosed(t, firstPublishDone, "first Publish")
	waitClosed(t, secondPublishDone, "second Publish")
}

// Each typed error matches its own sentinel only, without T, and still matches what it wraps.
func TestErrors_Is(t *testing.T) {
	t.Parallel()

	id := uint64(7)
	targets := []error{
		multicast.ErrSubscriberNotFound, subscriber.ErrClosed, subscriber.ErrTimeout, subscriber.ErrDropped,
		subscriber.ErrPanic, subscriber.ErrStore, subscriber.ErrEvicted, subscriber.ErrExpired, context.DeadlineExceeded,
		handleError(1),
	}
	for _, tc := range []struct {
		err     error
		matches []error
	}{
		{&multicast.SubscriberNotFoundError{SubscriberID: id}, []error{multicast.ErrSubscriberNotFound}},
		{&subscriber.ClosedError[int]{SubscriberID: id, Message: 1}, []error{subscriber.ErrClosed}},
		{
			&subscriber.TimeoutError[int]{SubscriberID: id, Message: 1, Err: context.DeadlineExceeded},
			[]error{subscriber.ErrTimeout, context.DeadlineExceeded},
		},
		{&subscriber.DroppedError[int]{SubscriberID: id, Message: 1}, []error{subscriber.ErrDropped}},
		{
			&subscriber.EvictedError[int]{
				SubscriberID: id, Message: 1,
				Err: &subscriber.TimeoutError[int]{SubscriberID: id, Message: 1, Err: context.DeadlineExceeded},
			},
			[]error{subscriber.ErrEvicted, subscriber.ErrTimeout, context.DeadlineExceeded},
		},
		{
			&subscriber.PanicError[int]{SubscriberID: id, Message: 1, Value: handleError(1)},
			[]error{subscriber.ErrPanic, handleError(1)},
		},
		{&subscriber.PanicError[int]{SubscriberID: id, Message: 1, Value: "not an error"}, []error{subscriber.ErrPanic}},
		{&subscriber.HandleError[int]{SubscriberID: id, Message: 1, Err: handleError(1)}, []error{handleError(1)}},
		{&subscriber.ExpiredError[int]{SubscriberID: id, Message: 1, Age: time.Second}, []error{subscriber.ErrExpired}},
		{
			&subscriber.StoreError[int]{
				SubscriberID: id, Message: 1, Err: handleError(1),
				Cause: &subscriber.DroppedError[int]{SubscriberID: id, Message: 1},
			},
			[]error{subscriber.ErrStore, handleError(1), subscriber.ErrDropped},
		},
	} {
		for _, target := range targets {
			if got, want := errors.Is(tc.err, target), slices.Contains(tc.matches, target); got != want {
				t.Errorf("errors.Is(%v, %v) = %t, want %t", tc.err, target, got, want)
			}
		}
		if s := tc.err.Error(); !strings.Contains(s, strconv.FormatUint(id, 10)) {
			t.Errorf("error %q does not mention the subscriber %d", s, id)
		}
	}
}

// message.WithSync overrides a default of message.WithParallel, message.WithAsync or message.WithNonBlocking.
func TestMessageWithSync(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		timeouts := &recorder[int]{}
		defaults := []message.Option[int]{
			message.WithParallel[int](), message.WithAsync[int](), message.WithNonBlocking[int](),
		}
		ids := make([]uint64, 0, len(defaults))
		stuck := make([]*recorder[int], 0, len(defaults))
		for _, delivery := range defaults {
			r := &recorder[int]{hold: make(chan struct{})}
			ids = append(ids, subscribe(t, g, r.handle,
				subscriber.WithDefaultMessageOptions(delivery),
				subscriber.WithErrorHandler[int](recordTimeouts(t, timeouts))))
			stuck = append(stuck, r)
		}
		synctest.Wait() // all are idle, so that the non-blocking one takes 0
		g.Publish(t.Context(), 0)
		synctest.Wait() // all are now processing 0 and not reading

		start := time.Now()
		n := g.Publish(t.Context(), 1, message.WithSync[int](), message.WithTimeout[int](time.Second))
		if elapsed, want := time.Since(start), time.Duration(len(defaults))*time.Second; elapsed != want {
			t.Errorf("sync Publish returned after %v, want %v: the whole timeout for each subscriber", elapsed, want)
		}
		if n != 0 {
			t.Errorf("Publish handed the message to %d subscribers, want 0", n)
		}
		if got, want := timeouts.messages(), slices.Repeat([]int{1}, len(defaults)); !slices.Equal(got, want) {
			t.Errorf("error handler got timeouts for messages %v, want %v", got, want)
		}

		unsubscribeAll(t, g, ids...)
		for _, r := range stuck {
			r.release()
		}
		synctest.Wait()
		for i, r := range stuck {
			if got, want := r.messages(), []int{0}; !slices.Equal(got, want) {
				t.Errorf("stuck subscriber %d received %v, want %v", i, got, want)
			}
		}
	})
}

// A non-blocking Publish hands the message to idle subscribers, and drops it for busy ones, telling their error
// handlers.
func TestMessageWithNonBlocking(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		stuck, stuckID := hold(t, g)
		reader, readerID := record(t, g)
		g.Publish(t.Context(), 0)
		synctest.Wait() // stuck is now processing 0 and not reading, and reader is idle

		errs := &recorder[error]{}
		start := time.Now()
		n := g.Publish(t.Context(), 1, message.WithNonBlocking[int](),
			message.WithErrorHandler[int](func(_ context.Context, err error) {
				errs.record(err)
			}))
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("non-blocking Publish returned after %v, want no wait", elapsed)
		}
		if n != 1 {
			t.Errorf("non-blocking Publish handed the message to %d subscribers, want 1: the idle one", n)
		}
		got := errs.messages()
		var dropped *subscriber.DroppedError[int]
		if len(got) != 1 || !errors.As(got[0], &dropped) || dropped.SubscriberID != stuckID || dropped.Message != 1 ||
			!errors.Is(got[0], subscriber.ErrDropped) {
			t.Errorf("message error handler got %v, want a single *DroppedError for subscriber %d and message 1",
				got, stuckID)
		}

		unsubscribeAll(t, g, stuckID, readerID)
		stuck.release()
		synctest.Wait()
		if got, want := stuck.messages(), []int{0}; !slices.Equal(got, want) {
			t.Errorf("stuck subscriber received %v, want %v", got, want)
		}
		if got, want := reader.messages(), []int{0, 1}; !slices.Equal(got, want) {
			t.Errorf("reader received %v, want %v", got, want)
		}
	})
}

// A parallel Publish reaches the others without waiting for a stuck one, and returns once that one took it too.
func TestMessageWithParallel(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		stuck, stuckID := hold(t, g)
		reader, readerID := record(t, g)
		g.Publish(t.Context(), 0)
		synctest.Wait() // stuck is now processing 0 and not reading, and reader is idle

		done := make(chan struct{})
		var n int
		go func() {
			defer close(done)
			n = g.Publish(t.Context(), 1, message.WithParallel[int]())
		}()
		synctest.Wait()
		if got, want := reader.messages(), []int{0, 1}; !slices.Equal(got, want) {
			t.Errorf("reader received %v while stuck was still processing, want %v", got, want)
		}
		select {
		case <-done:
			t.Error("parallel Publish returned before the stuck subscriber took the message")
		default:
		}

		stuck.release()
		waitClosed(t, done, "parallel Publish")
		if n != 2 {
			t.Errorf("parallel Publish handed the message to %d subscribers, want 2", n)
		}

		unsubscribeAll(t, g, stuckID, readerID)
		synctest.Wait()
		if got, want := stuck.messages(), []int{0, 1}; !slices.Equal(got, want) {
			t.Errorf("stuck subscriber received %v, want %v", got, want)
		}
	})
}

// Parallel Publish calls from one goroutine reach every subscriber in order, however slow, since each waits for all.
func TestMessageWithParallel_Order(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const messages = 50
		g := newGroup[int](t)
		delays := []time.Duration{time.Millisecond, 3 * time.Millisecond}
		ids := make([]uint64, 0, len(delays))
		recorders := make([]*recorder[int], 0, len(delays))
		for _, delay := range delays {
			r := &recorder[int]{}
			ids = append(ids, subscribe(t, g, func(ctx context.Context, id uint64, msg int) error {
				time.Sleep(delay)

				return r.handle(ctx, id, msg)
			}))
			recorders = append(recorders, r)
		}

		want := make([]int, 0, messages)
		for msg := range messages {
			if n := g.Publish(t.Context(), msg, message.WithParallel[int]()); n != len(delays) {
				t.Errorf("parallel Publish of %d handed it to %d subscribers, want %d", msg, n, len(delays))
			}
			want = append(want, msg)
		}
		unsubscribeAll(t, g, ids...)
		time.Sleep(slices.Max(delays)) // for each subscriber to be done with the last message, which it may have just taken
		synctest.Wait()

		for i, r := range recorders {
			if got := r.messages(); !slices.Equal(got, want) {
				t.Errorf("subscriber %d received %v, want %v", i, got, want)
			}
		}
	})
}

// With WithSerial, a Publish waits for the one under way to return, even a non-blocking one.
func TestMulticastWithSerial(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t, multicast.WithSerial[int]())
		stuck, stuckID := hold(t, g)
		// Buffered, so that it has room for the non-blocking Publish, however late it handles the one before.
		reader := &recorder[int]{}
		readerID := subscribe(t, g, reader.handle, subscriber.WithBuffer[int](2))
		g.Publish(t.Context(), 0)
		synctest.Wait() // stuck is now processing 0 and not reading

		first, second := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(first)
			g.Publish(t.Context(), 1)
		}()
		synctest.Wait() // the first Publish now waits on stuck
		go func() {
			defer close(second)
			g.Publish(t.Context(), 2, message.WithNonBlocking[int]())
		}()
		synctest.Wait()
		select {
		case <-second:
			t.Error("non-blocking Publish returned while the one before was under way")
		default:
		}
		if got := reader.messages(); slices.Contains(got, 2) {
			t.Errorf("reader received %v while the Publish before was under way, want no 2", got)
		}

		stuck.release()
		waitClosed(t, first, "first Publish")
		waitClosed(t, second, "non-blocking Publish")

		unsubscribeAll(t, g, stuckID, readerID)
		synctest.Wait()
		if got, want := reader.messages(), []int{0, 1, 2}; !slices.Equal(got, want) {
			t.Errorf("reader received %v, want %v", got, want)
		}
	})
}

// With WithSerial, parallel Publish calls from several goroutines reach every subscriber in the same order,
// however slow.
func TestMulticastWithSerial_Order(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const (
			goroutines = 5
			messages   = 10
		)
		g := newGroup(t, multicast.WithSerial[int]())
		delays := []time.Duration{time.Millisecond, 3 * time.Millisecond}
		ids := make([]uint64, 0, len(delays))
		recorders := make([]*recorder[int], 0, len(delays))
		for _, delay := range delays {
			r := &recorder[int]{}
			ids = append(ids, subscribe(t, g, func(ctx context.Context, id uint64, msg int) error {
				time.Sleep(delay)

				return r.handle(ctx, id, msg)
			}))
			recorders = append(recorders, r)
		}

		var wg sync.WaitGroup
		for j := range goroutines {
			wg.Go(func() {
				for i := range messages {
					g.Publish(t.Context(), j*messages+i, message.WithParallel[int]())
				}
			})
		}
		waitGroup(t, &wg, "Publish calls")
		unsubscribeAll(t, g, ids...)
		time.Sleep(slices.Max(delays)) // for each subscriber to be done with the last message, which it may have just taken
		synctest.Wait()

		want := recorders[0].messages()
		if len(want) != goroutines*messages {
			t.Fatalf("subscriber 0 received %d messages, want %d", len(want), goroutines*messages)
		}
		for i, r := range recorders[1:] {
			if got := r.messages(); !slices.Equal(got, want) {
				t.Errorf("subscriber %d received %v, want the order subscriber 0 received them in: %v", i+1, got, want)
			}
		}
	})
}

// WithDefaultSubscriberOptions builds each subscriber its own options, Subscribe's and SubscribeSeq's alike, which
// their own override.
func TestMulticastWithDefaultSubscriberOptions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		defaultErrs, ownErrs := &recorder[int]{}, &recorder[int]{}
		g := newGroup(t, multicast.WithDefaultSubscriberOptions(func() []subscriber.Option[int] {
			return []subscriber.Option[int]{
				// Shared, it would pass each value to the first subscriber only.
				subscriber.WithFilter(filter.Changed(func(prev, next int) bool { return prev != next })),
				subscriber.WithErrorHandler[int](recordFailures(t, defaultErrs)),
			}
		}))
		fail := func(_ context.Context, _ uint64, msg int) error { return handleError(msg) }
		defaultID := subscribe(t, g, fail)
		ownID := subscribe(t, g, fail, subscriber.WithErrorHandler[int](recordFailures(t, ownErrs)))
		seqID, seq := subscribeSeq(t, g)

		yielded := &recorder[int]{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for msg := range seq {
				yielded.record(msg)
			}
		}()
		for _, msg := range []int{1, 1, 2} {
			g.Publish(t.Context(), msg)
		}
		unsubscribeAll(t, g, defaultID, ownID, seqID)
		waitClosed(t, done, "SubscribeSeq loop")
		synctest.Wait()

		for _, c := range []struct {
			what string
			r    *recorder[int]
		}{
			{"the default error handler", defaultErrs},
			{"Subscribe's own error handler", ownErrs},
			{"the SubscribeSeq loop", yielded},
		} {
			if got, want := c.r.messages(), []int{1, 2}; !slices.Equal(got, want) {
				t.Errorf("%s got %v, want %v", c.what, got, want)
			}
		}
	})
}

// Close unsubscribes every subscriber, which still processes what it took, and every later call returns ErrClosed.
func TestClose(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		buffered := &recorder[int]{hold: make(chan struct{})}
		bufferedID := subscribe(t, g, buffered.handle, subscriber.WithBuffer[int](2))
		unbuffered, unbufferedID := record(t, g)
		seqID, seq := subscribeSeq(t, g)

		yielded := &recorder[int]{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for msg := range seq {
				yielded.record(msg)
			}
		}()
		for msg := 1; msg <= 3; msg++ {
			g.Publish(t.Context(), msg)
		}
		// buffered is processing 1, with 2 and 3 in its buffer.
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		waitClosed(t, done, "the loop to end once the Group is closed")
		buffered.release()
		synctest.Wait()

		for name, r := range map[string]*recorder[int]{"buffered": buffered, "unbuffered": unbuffered, "seq": yielded} {
			if got, want := r.messages(), []int{1, 2, 3}; !slices.Equal(got, want) {
				t.Errorf("%s subscriber received %v, want %v", name, got, want)
			}
		}

		if n := g.Publish(t.Context(), 4); n != 0 {
			t.Errorf("Publish after Close handed the message to %d subscribers, want 0", n)
		}
		for _, id := range []uint64{bufferedID, unbufferedID, seqID} {
			if err := g.Unsubscribe(t.Context(), id); !isNotFound(err) {
				t.Errorf("Unsubscribe(%d) after Close = %v, want *SubscriberNotFoundError", id, err)
			}
		}
		if _, err := g.Subscribe(t.Context(), unbuffered.handle); !errors.Is(err, multicast.ErrClosed) {
			t.Errorf("Subscribe after Close = %v, want ErrClosed", err)
		}
		if _, _, err := g.SubscribeSeq(t.Context()); !errors.Is(err, multicast.ErrClosed) {
			t.Errorf("SubscribeSeq after Close = %v, want ErrClosed", err)
		}
		if err := g.Close(); !errors.Is(err, multicast.ErrClosed) {
			t.Errorf("second Close = %v, want ErrClosed", err)
		}
	})
}

// handle closes the Group while a Publish waits on it: Close must not wait, and the Publish gives up.
func TestClose_FromHandle(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		g := newGroup[int](t)
		other, _ := record(t, g)

		self := &recorder[int]{}
		proceed := make(chan struct{})
		subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
			self.record(msg)
			if msg == 1 {
				<-proceed
				if err := g.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			}

			return nil
		})

		g.Publish(ctx, 1) // self is now processing 1 and not reading
		done := make(chan struct{})
		go func() {
			defer close(done)
			g.Publish(ctx, 2)
		}()
		synctest.Wait() // the Publish of 2 is waiting on self
		close(proceed)
		waitClosed(t, done, "Publish during which handle closed the Group")
		synctest.Wait()

		if got, want := self.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("closing subscriber received %v, want %v", got, want)
		}
		// other may come after self, already unsubscribed.
		if got := other.messages(); !slices.Equal(got, []int{1}) && !slices.Equal(got, []int{1, 2}) {
			t.Errorf("other subscriber received %v, want [1] or [1 2]", got)
		}
	})
}

// recorder is a handle that records every message, in order. With hold, handle blocks until it is closed, holding up
// the next Publish.
type recorder[T any] struct {
	hold chan struct{}

	mu  sync.Mutex
	got []T
}

func (r *recorder[T]) handle(_ context.Context, _ uint64, msg T) error {
	r.record(msg)
	if r.hold != nil {
		<-r.hold
	}

	return nil
}

// record records msg and returns how many messages have been recorded so far.
func (r *recorder[T]) record(msg T) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, msg)

	return len(r.got)
}

// messages returns everything recorded so far, in order.
func (r *recorder[T]) messages() []T {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.got)
}

// release lets handle return, now and for every message after.
func (r *recorder[T]) release() {
	close(r.hold)
}

type handleError int

func (e handleError) Error() string {
	return fmt.Sprintf("handling %d failed", int(e))
}

// recordFailures returns an error handler that records each handleError's message into r, and fails on any other error.
func recordFailures(t *testing.T, r *recorder[int]) func(context.Context, error) {
	t.Helper()

	return func(_ context.Context, err error) {
		var failed handleError
		if !errors.As(err, &failed) {
			t.Errorf("error handler got %v, want a handleError", err)

			return
		}
		r.record(int(failed))
	}
}

// recordTimeouts returns an error handler that records the message of each *TimeoutError wrapping
// context.DeadlineExceeded into r, and fails on any other error.
func recordTimeouts(t *testing.T, r *recorder[int]) func(context.Context, error) {
	t.Helper()

	return func(_ context.Context, err error) {
		var timeout *subscriber.TimeoutError[int]
		if !errors.As(err, &timeout) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error handler got %v, want a *TimeoutError wrapping %v", err, context.DeadlineExceeded)

			return
		}
		r.record(timeout.Message)
	}
}

// newGroup returns a Group closed in Cleanup, since its goroutine would otherwise outlive a synctest bubble.
func newGroup[T any](tb testing.TB, options ...multicast.Option[T]) *multicast.Group[T] {
	tb.Helper()
	g := multicast.New(options...)
	tb.Cleanup(func() { _ = g.Close() })

	return g
}

// subscribeCtx is the test's ctx, never done, so that a leaked subscriber fails the test.
func subscribeCtx(t *testing.T) context.Context {
	t.Helper()

	return context.WithoutCancel(t.Context())
}

func subscribe[T any](
	t *testing.T, g *multicast.Group[T], handle func(context.Context, uint64, T) error,
	options ...subscriber.Option[T],
) uint64 {
	t.Helper()
	id, err := g.Subscribe(subscribeCtx(t), handle, options...)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	return id
}

// record subscribes a recorder.
func record[T any](t *testing.T, g *multicast.Group[T]) (*recorder[T], uint64) {
	t.Helper()
	r := &recorder[T]{}
	id := subscribe(t, g, r.handle)

	return r, id
}

// hold subscribes a recorder that holds on to every message until it is released.
func hold[T any](t *testing.T, g *multicast.Group[T]) (*recorder[T], uint64) {
	t.Helper()
	r := &recorder[T]{hold: make(chan struct{})}
	id := subscribe(t, g, r.handle)

	return r, id
}

// subscribeSeq subscribes with SubscribeSeq, with subscribeCtx.
func subscribeSeq[T any](t *testing.T, g *multicast.Group[T], options ...subscriber.Option[T]) (uint64, iter.Seq2[T, func(error)]) {
	t.Helper()
	id, seq, err := g.SubscribeSeq(subscribeCtx(t), options...)
	if err != nil {
		t.Fatalf("SubscribeSeq: %v", err)
	}

	return id, seq
}

func unsubscribe[T any](t *testing.T, g *multicast.Group[T], id uint64) {
	t.Helper()
	if err := g.Unsubscribe(t.Context(), id); err != nil {
		t.Fatalf("Unsubscribe(%d): %v", id, err)
	}
}

func unsubscribeAll[T any](t *testing.T, g *multicast.Group[T], ids ...uint64) {
	t.Helper()
	for _, id := range ids {
		unsubscribe(t, g, id)
	}
}

// publish calls g.Publish and returns whatever it panicked with.
func publish[T any](ctx context.Context, g *multicast.Group[T], msg T) any {
	return recovered(func() { g.Publish(ctx, msg) })
}

// recovered runs f and returns whatever it panicked with, so that a panic fails only its own test.
func recovered(f func()) any {
	var r any
	func() {
		defer func() { r = recover() }()
		f()
	}()

	return r
}

// received returns what ch holds, without waiting.
func received[E any](ch <-chan E) []E {
	var got []E
	for {
		select {
		case e := <-ch:
			got = append(got, e)
		default:
			return got
		}
	}
}

func isNotFound(err error) bool {
	var notFound *multicast.SubscriberNotFoundError

	return errors.As(err, &notFound)
}

func waitClosed(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(deadlockTimeout):
		t.Fatalf("still waiting for %s after %v: deadlock", what, deadlockTimeout)
	}
}

func waitGroup(t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	waitClosed(t, done, what)
}
