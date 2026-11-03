package multicast_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast/middleware"
	"github.com/elojah/multicast/subscriber"
)

// Middlewares wrap handle in order, the first one outermost, across several options, and nil ones are skipped.
func TestSubscriberWithMiddleware(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		ctx := context.WithValue(subscribeCtx(t), key{}, "subscribe")
		g := newGroup[int](t)
		calls := &recorder[string]{}
		ids := &recorder[uint64]{}
		trace := func(name string) subscriber.Middleware[int] {
			return func(next subscriber.Handler[int]) subscriber.Handler[int] {
				return func(ctx context.Context, id uint64, msg int) error {
					if v := ctx.Value(key{}); v != "subscribe" {
						t.Errorf("middleware %s got a ctx with value %v, want the one passed to Subscribe", name, v)
					}
					ids.record(id)
					calls.record(fmt.Sprintf("%s before %d", name, msg))
					err := next(ctx, id, msg)
					calls.record(fmt.Sprintf("%s after %d", name, msg))

					return err
				}
			}
		}
		id, err := g.Subscribe(ctx, func(_ context.Context, id uint64, msg int) error {
			ids.record(id)
			calls.record(fmt.Sprintf("handle %d", msg))

			return nil
		},
			subscriber.WithMiddleware(trace("a"), nil, trace("b")),
			subscriber.WithMiddleware(trace("c")),
		)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		g.Publish(t.Context(), 1)
		unsubscribe(t, g, id)
		synctest.Wait()

		want := []string{"a before 1", "b before 1", "c before 1", "handle 1", "c after 1", "b after 1", "a after 1"}
		if got := calls.messages(); !slices.Equal(got, want) {
			t.Errorf("calls were %q, want %q", got, want)
		}
		if got, want := ids.messages(), slices.Repeat([]uint64{id}, 4); !slices.Equal(got, want) {
			t.Errorf("middlewares and handle got IDs %v, want the subscriber's, %d", got, id)
		}
	})
}

// With middleware.Recover first, a panic in a later middleware is a *PanicError, like one in handle, and the subscriber
// goes on.
func TestSubscriberWithMiddleware_Recover(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		handled := &recorder[int]{}
		errs := &recorder[error]{}
		panicOnOne := func(next subscriber.Handler[int]) subscriber.Handler[int] {
			return func(ctx context.Context, id uint64, msg int) error {
				if msg == 1 {
					panic(handleError(msg))
				}

				return next(ctx, id, msg)
			}
		}
		id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
			handled.record(msg)
			if msg == 2 {
				panic(handleError(msg))
			}

			return nil
		},
			subscriber.WithMiddleware(middleware.Recover[int](), panicOnOne),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) {
				errs.record(err)
			}),
		)

		for msg := 1; msg <= 3; msg++ {
			g.Publish(t.Context(), msg)
		}
		unsubscribe(t, g, id)
		synctest.Wait()

		if got, want := handled.messages(), []int{2, 3}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
		got := errs.messages()
		if len(got) != 2 {
			t.Fatalf("error handler got %v, want a *PanicError for each of messages 1 and 2", got)
		}
		for i, err := range got {
			msg := i + 1
			var panicErr *subscriber.PanicError[int]
			if !errors.As(err, &panicErr) {
				t.Errorf("error %d = %v, want a *PanicError", i, err)

				continue
			}
			if panicErr.SubscriberID != id || panicErr.Message != msg || panicErr.Value != handleError(msg) {
				t.Errorf("error %d is for subscriber %d and message %d with value %v, want %d, %d and %v",
					i, panicErr.SubscriberID, panicErr.Message, panicErr.Value, id, msg, handleError(msg))
			}
		}
	})
}

// Cancelling the Subscribe ctx during a middleware.Retry wait unsubscribes and ends the wait: the last error is
// reported at once, and the goroutine ends.
func TestSubscriberWithMiddleware_RetryContextDone(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		g := newGroup[int](t)
		handled := &recorder[int]{}
		failures := &recorder[int]{}
		_, err := g.Subscribe(ctx, func(_ context.Context, _ uint64, msg int) error {
			handled.record(msg)

			return handleError(msg)
		},
			subscriber.WithMiddleware(middleware.Retry[int](middleware.RetryPolicy{Attempts: 3, Delay: time.Hour})),
			subscriber.WithErrorHandler[int](recordFailures(t, failures)),
		)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		start := time.Now()
		g.Publish(t.Context(), 1)
		synctest.Wait()
		cancel()
		// Waits for the subscriber's goroutine to report, but lets no time pass: a Retry still waiting would not have.
		synctest.Wait()

		if got, want := failures.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("error handler got failures for %v, want %v", got, want)
		}
		if got, want := handled.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("%v passed, want none", elapsed)
		}
		if n := g.Publish(t.Context(), 2); n != 0 {
			t.Errorf("Publish after cancel reached %d subscribers, want 0", n)
		}
	})
}

// Sharing a store, middleware.History and subscriber.WithDeadLetters put each message in it once: those handled with a
// nil Err, the others with their error.
func TestSubscriberWithMiddleware_History(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		history := &recordStore{}
		id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
			switch msg {
			case 2:
				return handleError(msg)
			case 3:
				panic(handleError(msg))
			default:
				return nil
			}
		},
			subscriber.WithMiddleware(middleware.Recover[int](), middleware.History[int](history)),
			subscriber.WithDeadLetters[int](history),
		)

		for msg := 1; msg <= 4; msg++ {
			g.Publish(t.Context(), msg)
		}
		unsubscribe(t, g, id)
		synctest.Wait()

		records := history.messages()
		if len(records) != 4 {
			t.Fatalf("store got %v, want a single record for each of messages 1 to 4", records)
		}
		// All from the subscriber's goroutine, so in message order.
		for i, want := range []error{nil, handleError(2), subscriber.ErrPanic, nil} {
			msg := i + 1
			if r := records[i]; r.SubscriberID != id || r.Message != msg || !errors.Is(r.Err, want) {
				t.Errorf("record %d is for subscriber %d and message %d with error %v, want %d, %d and %v",
					i, r.SubscriberID, r.Message, r.Err, id, msg, want)
			}
		}
	})
}

// Middlewares wrap a SubscribeSeq loop body like handle: they get the error passed to fail, and Retry yields the
// message again. Once the loop has ended, the message is a *subscriber.ClosedError, which Retry does not retry.
func TestSubscriberWithMiddleware_SubscribeSeq(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		results := &recorder[string]{}
		spy := func(next subscriber.Handler[int]) subscriber.Handler[int] {
			return func(ctx context.Context, id uint64, msg int) error {
				err := next(ctx, id, msg)
				result := "ok"
				switch {
				case errors.Is(err, subscriber.ErrClosed):
					result = "closed"
				case err != nil:
					result = err.Error()
				}
				results.record(fmt.Sprintf("%d: %s", msg, result))

				return err
			}
		}
		errs := &recorder[error]{}
		_, seq := subscribeSeq(t, g,
			subscriber.WithMiddleware(middleware.Retry[int](middleware.RetryPolicy{Attempts: 3}), spy),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) { errs.record(err) }))

		go func() {
			g.Publish(t.Context(), 1)
			g.Publish(t.Context(), 2)
		}()
		var got []int
		for msg, fail := range seq {
			got = append(got, msg)
			// 1 fails once, then succeeds. 2 fails, and the loop breaks before Retry can yield it again.
			if len(got) == 1 || msg == 2 {
				fail(handleError(msg))
			}
			if msg == 2 {
				break
			}
		}
		synctest.Wait()

		if want := []int{1, 1, 2}; !slices.Equal(got, want) {
			t.Errorf("loop got %v, want %v", got, want)
		}
		want := []string{"1: handling 1 failed", "1: ok", "2: handling 2 failed", "2: closed"}
		if got := results.messages(); !slices.Equal(got, want) {
			t.Errorf("middleware got %q, want %q", got, want)
		}
		if got := errs.messages(); len(got) != 1 || !errors.Is(got[0], subscriber.ErrClosed) {
			t.Errorf("error handler got %v, want a single *subscriber.ClosedError", got)
		}
	})
}

// With middleware.FirstN, the first subscribers to handle a message win: the others stop, a Retry wait inside FirstN
// included, and report nothing. Each counts the message as handled.
func TestSubscriberWithMiddleware_FirstN(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		type job struct{ race *middleware.Race }
		g := newGroup[job](t)
		errs := &recorder[error]{}
		racing := subscriber.WithMiddleware(
			middleware.Recover[job](),
			middleware.FirstN(2, func(j job) *middleware.Race { return j.race }),
			middleware.Retry[job](middleware.RetryPolicy{Attempts: 3, Delay: time.Hour}),
		)
		subscribe := func(handle func(ctx context.Context) error) uint64 {
			t.Helper()

			return subscribe(t, g, func(ctx context.Context, _ uint64, _ job) error { return handle(ctx) },
				racing, subscriber.WithErrorHandler[job](func(_ context.Context, err error) { errs.record(err) }))
		}
		cancelled := make(chan error, 1)
		subscribe(func(ctx context.Context) error {
			<-ctx.Done()
			cancelled <- ctx.Err()

			return ctx.Err()
		})
		first := subscribe(func(context.Context) error {
			time.Sleep(time.Second)

			return nil
		})
		second := subscribe(func(context.Context) error {
			time.Sleep(2 * time.Second)

			return nil
		})
		subscribe(func(context.Context) error { return handleError(1) })

		msg := job{race: middleware.NewRace()}
		start := time.Now()
		if n := g.Publish(t.Context(), msg); n != 4 {
			t.Fatalf("Publish returned %d, want 4", n)
		}
		<-msg.race.Done()
		if elapsed, winners := time.Since(start), msg.race.Winners(); elapsed != 2*time.Second || !slices.Equal(winners, []uint64{first, second}) {
			t.Errorf("subscribers %v won after %v, want %d and %d after %v", winners, elapsed, first, second, 2*time.Second)
		}
		if err := <-cancelled; !errors.Is(err, context.Canceled) {
			t.Errorf("loser's ctx ended with %v, want %v", err, context.Canceled)
		}
		// Lets no time pass: a Retry still waiting would not have returned.
		synctest.Wait()

		if got := errs.messages(); len(got) != 0 {
			t.Errorf("error handlers got %v, want none", got)
		}
		for _, s := range g.Stats() {
			if s.Handled != 1 || s.Failed != 0 || s.Handling != 0 {
				t.Errorf("subscriber %d: %+v, want the message handled and nothing running", s.SubscriberID, s)
			}
		}
	})
}

// A subscriber busy while the race ends skips the message once it gets to it: handle never sees it, nothing is reported,
// and it counts as handled.
func TestSubscriberWithMiddleware_FirstNSkip(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		race := middleware.NewRace()
		racing := subscriber.WithMiddleware(middleware.FirstN(1, func(msg int) *middleware.Race {
			if msg == 2 {
				return race
			}

			return nil
		}))
		g := newGroup[int](t)
		errs := &recorder[error]{}
		busy := &recorder[int]{hold: make(chan struct{})}
		subscribe(t, g, busy.handle, racing, subscriber.WithBuffer[int](1),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) { errs.record(err) }))
		winner := subscribe(t, g, func(context.Context, uint64, int) error { return nil }, racing)

		g.Publish(t.Context(), 1)
		g.Publish(t.Context(), 2)
		<-race.Done()
		busy.release()
		synctest.Wait()

		if winners := race.Winners(); !slices.Equal(winners, []uint64{winner}) {
			t.Errorf("Winners returned %v, want [%d]", winners, winner)
		}
		if got, want := busy.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("busy subscriber handled %v, want %v", got, want)
		}
		if got := errs.messages(); len(got) != 0 {
			t.Errorf("error handler got %v, want none", got)
		}
		for _, s := range g.Stats() {
			if s.Handled != 2 || s.Failed != 0 || s.Handling != 0 {
				t.Errorf("subscriber %d: %+v, want both messages handled and nothing running", s.SubscriberID, s)
			}
		}
	})
}

// A subscriber failing before the race ends has not lost: its error is reported and dead-lettered, and it counts as
// failed, though another wins later.
func TestSubscriberWithMiddleware_FirstNFailed(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		race := middleware.NewRace()
		racing := subscriber.WithMiddleware(middleware.FirstN(1, func(int) *middleware.Race { return race }))
		g := newGroup[int](t)
		dead := &recordStore{}
		letters := subscriber.WithDeadLetters[int](dead)
		failed := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error { return handleError(msg) }, racing, letters)
		winner := subscribe(t, g, func(context.Context, uint64, int) error {
			time.Sleep(time.Second)

			return nil
		}, racing, letters)

		g.Publish(t.Context(), 1)
		<-race.Done()
		synctest.Wait()

		records := dead.messages()
		if len(records) != 1 || records[0].SubscriberID != failed || !errors.Is(records[0].Err, handleError(1)) {
			t.Errorf("store got %v, want a single record of subscriber %d with %v", records, failed, handleError(1))
		}
		if winners := race.Winners(); !slices.Equal(winners, []uint64{winner}) {
			t.Errorf("Winners returned %v, want [%d]", winners, winner)
		}
		for _, s := range g.Stats() {
			if want := s.SubscriberID == failed; (s.Failed == 1) != want || (s.Handled == 1) == want {
				t.Errorf("subscriber %d: %+v, want the message failed only for %d, and handled otherwise", s.SubscriberID, s, failed)
			}
		}
	})
}

// A Retry outside FirstN, which is where a default one sits, cannot see the race: the subscriber waits out the delay,
// and its next attempt skips the message. Nothing is reported.
func TestSubscriberWithMiddleware_FirstNInsideRetry(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		race := middleware.NewRace()
		first := middleware.FirstN(1, func(int) *middleware.Race { return race })
		g := newGroup[int](t)
		attempts := &recorder[int]{}
		failures := &recorder[int]{}
		retrying := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
			attempts.record(msg)

			return handleError(msg)
		},
			subscriber.WithMiddleware(middleware.Retry[int](middleware.RetryPolicy{Attempts: 3, Delay: time.Hour}), first),
			subscriber.WithErrorHandler[int](recordFailures(t, failures)),
		)
		subscribe(t, g, func(context.Context, uint64, int) error {
			time.Sleep(time.Second)

			return nil
		}, subscriber.WithMiddleware(first))

		g.Publish(t.Context(), 1)
		<-race.Done()
		synctest.Wait()
		handling := func() time.Duration {
			t.Helper()
			for _, s := range g.Stats() {
				if s.SubscriberID == retrying {
					return s.Handling
				}
			}
			t.Fatalf("Stats has no subscriber %d", retrying)

			return 0
		}
		if got := handling(); got != time.Second {
			t.Errorf("once the race is over, subscriber has been handling for %v, want %v, in its Retry's wait", got, time.Second)
		}

		time.Sleep(time.Hour)
		synctest.Wait()

		if got := handling(); got != 0 {
			t.Errorf("once the delay is over, subscriber has been handling for %v, want it idle", got)
		}
		if got, want := attempts.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v, the next attempt skipping it", got, want)
		}
		if got := failures.messages(); len(got) != 0 {
			t.Errorf("error handler got failures for %v, want none", got)
		}
	})
}

// A SubscribeSeq loop's body gets no ctx, so the race cannot cancel it: it can watch Race.Done, and does not win once
// the race is over. It never sees a message whose race ended first.
func TestSubscriberWithMiddleware_FirstNSeq(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		races := map[int]*middleware.Race{1: middleware.NewRace(), 2: middleware.NewRace()}
		racing := subscriber.WithMiddleware(middleware.FirstN(1, func(msg int) *middleware.Race { return races[msg] }))
		g := newGroup[int](t)
		_, seq := subscribeSeq(t, g, racing)
		seen := &recorder[int]{}
		go func() {
			for msg := range seq {
				seen.record(msg)
				<-races[msg].Done()
			}
		}()
		winner := subscribe(t, g, func(context.Context, uint64, int) error {
			time.Sleep(time.Second)

			return nil
		}, racing)

		g.Publish(t.Context(), 1)
		<-races[1].Done()
		synctest.Wait()
		if winners := races[1].Winners(); !slices.Equal(winners, []uint64{winner}) {
			t.Errorf("Winners returned %v, want [%d]", winners, winner)
		}

		races[2].Stop()
		g.Publish(t.Context(), 2)
		synctest.Wait()
		if got, want := seen.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("loop got %v, want %v", got, want)
		}
	})
}
