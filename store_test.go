package multicast_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/middleware"
	"github.com/elojah/multicast/subscriber"
)

var errPut = errors.New("put failed")

// The store gets every message handle fails on, with the subscriber's ID and handle's error, right before the error
// handler. Messages handled are not stored.
func TestSubscriberWithDeadLetters(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		events := &recorder[string]{}
		stored := &recordStore{}
		id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
			if msg%2 == 0 {
				return handleError(msg)
			}
			events.record(fmt.Sprintf("handled %d", msg))

			return nil
		},
			subscriber.WithDeadLetters[int](storeFunc(func(ctx context.Context, r subscriber.Record[int]) error {
				events.record(fmt.Sprintf("stored %d", r.Message))

				return stored.Put(ctx, r)
			})),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) {
				events.record(fmt.Sprintf("reported %v", err))
			}),
		)

		for msg := 1; msg <= 4; msg++ {
			g.Publish(t.Context(), msg)
		}
		unsubscribe(t, g, id)
		synctest.Wait()

		want := []string{
			"handled 1", "stored 2", "reported " + handleError(2).Error(),
			"handled 3", "stored 4", "reported " + handleError(4).Error(),
		}
		if got := events.messages(); !slices.Equal(got, want) {
			t.Errorf("got events %q, want %q", got, want)
		}
		records := stored.messages()
		if len(records) != 2 {
			t.Fatalf("store got %v, want a record for each of messages 2 and 4", records)
		}
		for i, r := range records {
			msg := 2 * (i + 1)
			if r.SubscriberID != id || r.Message != msg || !errors.Is(r.Err, handleError(msg)) {
				t.Errorf("record %d is for subscriber %d and message %d with error %v, want %d, %d and %v",
					i, r.SubscriberID, r.Message, r.Err, id, msg, handleError(msg))
			}
		}
	})
}

// The store gets every lost message, whatever the reason, with the error the error handlers would get, even without
// one.
func TestSubscriberWithDeadLetters_Lost(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		want error
		// lose makes a subscriber with the store option lose message 1, and returns its ID.
		lose func(t *testing.T, g *multicast.Group[int], store subscriber.Option[int]) uint64
	}{
		{"handle", handleError(1), func(t *testing.T, g *multicast.Group[int], store subscriber.Option[int]) uint64 {
			t.Helper()
			id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
				return handleError(msg)
			}, store)
			g.Publish(t.Context(), 1)
			unsubscribe(t, g, id)

			return id
		}},
		{"Panic", subscriber.ErrPanic, func(t *testing.T, g *multicast.Group[int], store subscriber.Option[int]) uint64 {
			t.Helper()
			id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
				panic(handleError(msg))
			}, store, subscriber.WithMiddleware(middleware.Recover[int]()))
			g.Publish(t.Context(), 1)
			unsubscribe(t, g, id)

			return id
		}},
		{"Timeout", subscriber.ErrTimeout, func(t *testing.T, g *multicast.Group[int], store subscriber.Option[int]) uint64 {
			t.Helper()
			stuck := &recorder[int]{hold: make(chan struct{})}
			id := subscribe(t, g, stuck.handle, store)
			g.Publish(t.Context(), 0) // stuck is now processing 0 and not reading
			g.Publish(t.Context(), 1, message.WithTimeout[int](time.Second))
			unsubscribe(t, g, id)
			stuck.release()

			return id
		}},
		{"Dropped", subscriber.ErrDropped, func(t *testing.T, g *multicast.Group[int], store subscriber.Option[int]) uint64 {
			t.Helper()
			stuck := &recorder[int]{hold: make(chan struct{})}
			id := subscribe(t, g, stuck.handle, store)
			g.Publish(t.Context(), 0) // stuck is now processing 0 and not reading
			g.Publish(t.Context(), 1, message.WithNonBlocking[int]())
			unsubscribe(t, g, id)
			stuck.release()

			return id
		}},
		{"UnsubscribeDiscard", subscriber.ErrClosed, func(t *testing.T, g *multicast.Group[int], store subscriber.Option[int]) uint64 {
			t.Helper()
			stuck := &recorder[int]{hold: make(chan struct{})}
			id := subscribe(t, g, stuck.handle, store, subscriber.WithBuffer[int](1))
			g.Publish(t.Context(), 0) // stuck holds on to 0, and 1 fills the buffer
			g.Publish(t.Context(), 1)
			synctest.Wait()
			if err := g.Unsubscribe(t.Context(), id, subscriber.WithUnsubscribeDiscard()); err != nil {
				t.Fatalf("Unsubscribe: %v", err)
			}
			stuck.release()

			return id
		}},
		{"SubscribeSeq", subscriber.ErrClosed, func(t *testing.T, g *multicast.Group[int], store subscriber.Option[int]) uint64 {
			t.Helper()
			id, seq := subscribeSeq(t, g, subscriber.WithBuffer[int](2), store)
			g.Publish(t.Context(), 0)
			g.Publish(t.Context(), 1)
			for range seq {
				break // 1 is left in the buffer
			}

			return id
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup[int](t)
				stored := &recordStore{}
				id := tt.lose(t, g, subscriber.WithDeadLetters[int](stored))
				synctest.Wait()

				records := stored.messages()
				if len(records) != 1 {
					t.Fatalf("store got %v, want a single record for message 1", records)
				}
				if r := records[0]; r.SubscriberID != id || r.Message != 1 || !errors.Is(r.Err, tt.want) {
					t.Errorf("store got a record for subscriber %d and message %d with error %v, want %d, 1 and %v",
						r.SubscriberID, r.Message, r.Err, id, tt.want)
				}
			})
		})
	}
}

// When Put fails, the error handlers get a *StoreError, once, matching both ErrStore and the original error.
func TestSubscriberWithDeadLetters_PutFails(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		stored := &recordStore{err: errPut}
		failures := &recorder[int]{}
		recordFailure := recordFailures(t, failures)
		errs := &recorder[error]{}
		id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
			return handleError(msg)
		},
			subscriber.WithDeadLetters[int](stored),
			subscriber.WithErrorHandler[int](func(ctx context.Context, err error) {
				errs.record(err)
				recordFailure(ctx, err)
			}),
		)

		g.Publish(t.Context(), 1)
		g.Publish(t.Context(), 2)
		unsubscribe(t, g, id)
		synctest.Wait()

		if got, want := failures.messages(), []int{1, 2}; !slices.Equal(got, want) {
			t.Errorf("error handler found handleErrors for messages %v, want %v", got, want)
		}
		if got := stored.messages(); len(got) != 2 {
			t.Errorf("store got %v, want a single record for each of messages 1 and 2", got)
		}
		for i, err := range errs.messages() {
			msg := i + 1
			var storeErr *subscriber.StoreError[int]
			if !errors.As(err, &storeErr) || !errors.Is(err, subscriber.ErrStore) {
				t.Errorf("error %d = %v, want a *StoreError", i, err)

				continue
			}
			if storeErr.SubscriberID != id || storeErr.Message != msg || !errors.Is(storeErr.Err, errPut) ||
				!errors.Is(storeErr.Cause, handleError(msg)) {
				t.Errorf("error %d is for subscriber %d and message %d, failed with %v storing %v, want %d, %d, %v and %v",
					i, storeErr.SubscriberID, storeErr.Message, storeErr.Err, storeErr.Cause, id, msg, errPut, handleError(msg))
			}
		}
	})
}

// Put gets the values of the message's ctx, but never a done ctx, even when that one is.
func TestSubscriberWithDeadLetters_Context(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		ctx, cancel := context.WithCancel(context.WithValue(subscribeCtx(t), key{}, "subscribe"))
		messageCtx, cancelMessage := context.WithCancel(context.WithValue(t.Context(), key{}, "message"))
		g := newGroup[int](t)
		handled := &recorder[int]{hold: make(chan struct{})}
		puts := &recorder[string]{}
		_, err := g.Subscribe(ctx, handled.handle, subscriber.WithBuffer[int](2),
			subscriber.WithUnsubscribeOptions[int](subscriber.WithUnsubscribeDiscard()),
			subscriber.WithDeadLetters[int](storeFunc(func(ctx context.Context, r subscriber.Record[int]) error {
				puts.record(fmt.Sprintf("%d: %v, %v", r.Message, ctx.Value(key{}), ctx.Err()))

				return nil
			})))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		// handle holds on to 0, and 1 and 2 fill the buffer.
		g.Publish(t.Context(), 0)
		g.Publish(t.Context(), 1)
		g.Publish(t.Context(), 2, message.WithContext[int](messageCtx))
		cancelMessage()
		cancel()
		synctest.Wait()
		handled.release()
		synctest.Wait()

		if got, want := handled.messages(), []int{0}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
		if got, want := puts.messages(), []string{"1: subscribe, <nil>", "2: message, <nil>"}; !slices.Equal(got, want) {
			t.Errorf("Put got messages with ctx values and errors %q, want %q", got, want)
		}
	})
}

// Every message is either handled or stored, never both and never twice, whatever the delivery mode and the loss.
func TestSubscriberWithDeadLetters_ExactlyOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		stored := &recordStore{}
		options := []subscriber.Option[int]{
			subscriber.WithBuffer[int](1),
			subscriber.WithDeadLetters[int](stored),
			subscriber.WithUnsubscribeOptions[int](subscriber.WithUnsubscribeDiscard()),
		}
		handled := &recorder[int]{}
		handleID := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
			time.Sleep(time.Millisecond)
			if msg%3 == 0 {
				return handleError(msg)
			}
			handled.record(msg)

			return nil
		}, options...)
		yielded := &recorder[int]{}
		seqID, seq := subscribeSeq(t, g, options...)
		loopDone := make(chan struct{})
		go func() {
			defer close(loopDone)
			for msg := range seq {
				time.Sleep(time.Millisecond)
				yielded.record(msg)
			}
		}()

		const publishers, perPublisher = 4, 50
		deliveries := []message.Option[int]{
			message.WithSync[int](), message.WithParallel[int](), message.WithAsync[int](), message.WithNonBlocking[int](),
		}
		var wg sync.WaitGroup
		for j := range publishers {
			wg.Go(func() {
				for i := range perPublisher {
					g.Publish(t.Context(), j*perPublisher+i, deliveries[i%len(deliveries)],
						message.WithTimeout[int](time.Millisecond))
				}
			})
		}
		waitGroup(t, &wg, "publishers")
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		waitClosed(t, loopDone, "SubscribeSeq loop ended by Close")
		time.Sleep(time.Millisecond) // until handle is done with the message it took, which synctest.Wait does not wait for
		synctest.Wait()

		all := make([]int, publishers*perPublisher)
		for i := range all {
			all[i] = i
		}
		records := stored.messages()
		for id, got := range map[uint64][]int{handleID: handled.messages(), seqID: yielded.messages()} {
			var lost []int
			for _, r := range records {
				if r.SubscriberID != id {
					continue
				}
				lost = append(lost, r.Message)
				if !errors.Is(r.Err, subscriber.ErrTimeout) && !errors.Is(r.Err, subscriber.ErrDropped) &&
					!errors.Is(r.Err, subscriber.ErrClosed) && !errors.Is(r.Err, handleError(r.Message)) {
					t.Errorf("subscriber %d: message %d stored with error %v, want a handleError, *TimeoutError, "+
						"*DroppedError or *ClosedError", id, r.Message, r.Err)
				}
			}
			if len(got) == 0 || len(lost) == 0 {
				t.Errorf("subscriber %d handled %d messages and lost %d, want some of each", id, len(got), len(lost))
			}
			if both := slices.Sorted(slices.Values(append(slices.Clone(got), lost...))); !slices.Equal(both, all) {
				t.Errorf("subscriber %d: handled %v and stored %v, want every message from 0 to %d once",
					id, got, lost, len(all)-1)
			}
		}
		for _, r := range records {
			if r.SubscriberID != handleID && r.SubscriberID != seqID {
				t.Errorf("store got a record for subscriber %d, want %d or %d", r.SubscriberID, handleID, seqID)
			}
		}
	})
}

// recordStore is a subscriber.Store that records every Record it is given, in order, and fails with err if set.
type recordStore struct {
	recorder[subscriber.Record[int]]

	err error
}

func (s *recordStore) Put(_ context.Context, r subscriber.Record[int]) error {
	s.record(r)

	return s.err
}

// storeFunc makes a func a subscriber.Store.
type storeFunc func(ctx context.Context, r subscriber.Record[int]) error

func (f storeFunc) Put(ctx context.Context, r subscriber.Record[int]) error {
	return f(ctx, r)
}

// The replay comes before any Publish, in order, through the middlewares, error handlers and dead letters, with the
// default message options, each value counted like a message and acked once handled or reported.
func TestSubscriberWithReplay(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		events := &recorder[string]{}
		middleware := func(next subscriber.Handler[int]) subscriber.Handler[int] {
			return func(ctx context.Context, id uint64, msg int) error {
				events.record(fmt.Sprintf("middleware %d", msg))

				return next(ctx, id, msg)
			}
		}
		id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
			if msg%2 == 0 {
				return handleError(msg)
			}

			return nil
		},
			subscriber.WithReplay(replayOf(events, 1, 2, 3)),
			subscriber.WithMiddleware(middleware),
			subscriber.WithDeadLetters[int](storeFunc(func(_ context.Context, r subscriber.Record[int]) error {
				events.record(fmt.Sprintf("stored %d", r.Message))

				return nil
			})),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) { events.record("reported " + err.Error()) }),
			subscriber.WithDefaultMessageOptions(message.WithErrorHandler[int](func(_ context.Context, err error) {
				events.record("message reported " + err.Error())
			})),
		)

		// It waits until the subscriber has replayed 1 to 3, and counts none of them.
		if n := g.Publish(t.Context(), 4); n != 1 {
			t.Errorf("Publish handed 4 to %d subscribers, want 1", n)
		}
		synctest.Wait()
		want := []string{
			"middleware 1", "acked 1",
			"middleware 2", "stored 2", "reported handling 2 failed", "message reported handling 2 failed", "acked 2",
			"middleware 3", "acked 3",
			"middleware 4", "stored 4", "reported handling 4 failed", "message reported handling 4 failed",
		}
		if got := events.messages(); !slices.Equal(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
		checkStats(t, g, id, subscriber.Stats{Delivered: 4, Handled: 2, Failed: 2})

		unsubscribe(t, g, id)
		synctest.Wait()
	})
}

// Once the subscriber discards, yield returns false without handling the value, and the replay keeps the rest. Without
// discard, it keeps replaying once unsubscribed, and Shutdown waits for it.
func TestSubscriberWithReplay_Unsubscribe(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		remove func(t *testing.T, g *multicast.Group[int], id uint64) <-chan error
		want   []string
	}{
		{"Discard", func(t *testing.T, g *multicast.Group[int], id uint64) <-chan error {
			t.Helper()
			if err := g.Unsubscribe(t.Context(), id, subscriber.WithUnsubscribeDiscard()); err != nil {
				t.Fatalf("Unsubscribe: %v", err)
			}

			return nil
		}, []string{"handled 1", "acked 1", "stopped at 2"}},
		{"Shutdown", func(t *testing.T, g *multicast.Group[int], _ uint64) <-chan error {
			t.Helper()
			errs := shutdown(t.Context(), g)
			checkWaiting(t, errs, "the subscriber replays")

			return errs
		}, []string{"handled 1", "acked 1", "handled 2", "acked 2", "handled 3", "acked 3"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup[int](t)
				events := &recorder[string]{}
				hold := make(chan struct{})
				id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
					events.record(fmt.Sprintf("handled %d", msg))
					<-hold

					return nil
				},
					subscriber.WithReplay(replayOf(events, 1, 2, 3)),
					subscriber.WithErrorHandler[int](func(_ context.Context, err error) { t.Errorf("error handler got %v", err) }),
				)

				synctest.Wait() // handle holds on to 1
				errs := tt.remove(t, g, id)
				close(hold)
				if errs != nil {
					if err := waitShutdown(t, errs); err != nil {
						t.Fatalf("Shutdown: %v", err)
					}
				}
				synctest.Wait()
				if got := events.messages(); !slices.Equal(got, tt.want) {
					t.Errorf("got %q, want %q", got, tt.want)
				}
			})
		})
	}
}

// replayOf returns a replay of values that records "acked <value>" into events once yield returned true for it, and
// "stopped at <value>" once it returned false.
func replayOf(events *recorder[string], values ...int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for _, v := range values {
			if !yield(v) {
				events.record(fmt.Sprintf("stopped at %d", v))

				return
			}
			events.record(fmt.Sprintf("acked %d", v))
		}
	}
}
