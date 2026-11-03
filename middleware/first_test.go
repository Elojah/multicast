package middleware_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast/middleware"
	"github.com/elojah/multicast/subscriber"
)

// job is a message carrying its race.
type job struct {
	race *middleware.Race
}

func raceOf(j job) *middleware.Race { return j.race }

// The first handler to return nil wins: a running one is cancelled, its error dropped, and a later one skips the
// message.
func TestFirstN(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		first := middleware.FirstN(1, raceOf)
		msg := job{race: middleware.NewRace()}

		cancelled := make(chan error, 1)
		lost := make(chan error, 1)
		go func() {
			lost <- first(func(ctx context.Context, _ uint64, _ job) error {
				<-ctx.Done()
				cancelled <- ctx.Err()

				return ctx.Err()
			})(t.Context(), 1, msg)
		}()
		err := first(func(context.Context, uint64, job) error {
			time.Sleep(time.Second)

			return nil
		})(t.Context(), 2, msg)
		if err != nil {
			t.Errorf("winner returned %v, want nil", err)
		}
		if !done(msg.race) {
			t.Error("Done is open once a handler has won")
		}
		if winners := msg.race.Winners(); !slices.Equal(winners, []uint64{2}) {
			t.Errorf("Winners returned %v, want [2]", winners)
		}
		if err := <-cancelled; !errors.Is(err, context.Canceled) {
			t.Errorf("loser's ctx ended with %v, want %v", err, context.Canceled)
		}
		if err := <-lost; err != nil {
			t.Errorf("loser returned %v, want nil", err)
		}
		if err := t.Context().Err(); err != nil {
			t.Errorf("the ctx the loser got ended with %v, want it left alone", err)
		}

		calls := 0
		late := first(func(context.Context, uint64, job) error {
			calls++

			return errHandle
		})
		if err := late(t.Context(), 3, msg); err != nil || calls != 0 {
			t.Errorf("once won, a handler returned %v after %d calls, want nil after none", err, calls)
		}
	})
}

// With n = 2, only the second winner cancels the third handler.
func TestFirstN_Two(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		first := middleware.FirstN(2, raceOf)
		msg := job{race: middleware.NewRace()}
		start := time.Now()

		cancelled := make(chan time.Duration, 1)
		lost := make(chan error, 1)
		go func() {
			lost <- first(func(ctx context.Context, _ uint64, _ job) error {
				<-ctx.Done()
				cancelled <- time.Since(start)

				return ctx.Err()
			})(t.Context(), 1, msg)
		}()
		won := make(chan error, 1)
		go func() {
			won <- first(func(context.Context, uint64, job) error {
				time.Sleep(time.Second)

				return nil
			})(t.Context(), 2, msg)
		}()
		err := first(func(context.Context, uint64, job) error {
			time.Sleep(2 * time.Second)

			return nil
		})(t.Context(), 3, msg)

		if err := errors.Join(err, <-won); err != nil {
			t.Errorf("winners returned %v, want nil", err)
		}
		if winners := msg.race.Winners(); !slices.Equal(winners, []uint64{2, 3}) {
			t.Errorf("Winners returned %v, want [2 3]", winners)
		}
		if elapsed := <-cancelled; elapsed != 2*time.Second {
			t.Errorf("loser's ctx was cancelled after %v, want %v, once the second has won", elapsed, 2*time.Second)
		}
		if err := <-lost; err != nil {
			t.Errorf("loser returned %v, want nil", err)
		}
	})
}

// A handler failing before the race is over returns its error, and does not win.
func TestFirstN_Error(t *testing.T) {
	t.Parallel()

	msg := job{race: middleware.NewRace()}
	if err := middleware.FirstN(1, raceOf)(func(context.Context, uint64, job) error { return errHandle })(t.Context(), 1, msg); !errors.Is(err, errHandle) {
		t.Errorf("FirstN returned %v, want %v", err, errHandle)
	}
	if done(msg.race) {
		t.Errorf("Done is closed, with winners %v, though no handler returned nil", msg.race.Winners())
	}
}

// A message without a race reaches the handler as is, with its ctx.
func TestFirstN_NoRace(t *testing.T) {
	t.Parallel()

	type key struct{}
	ctx := context.WithValue(t.Context(), key{}, "subscribe")
	calls := 0
	handle := middleware.FirstN(1, raceOf)(func(got context.Context, _ uint64, _ job) error {
		calls++
		if got != ctx {
			t.Error("handler got another ctx than the one passed")
		}

		return errHandle
	})
	for range 2 {
		if err := handle(ctx, 1, job{}); !errors.Is(err, errHandle) {
			t.Errorf("FirstN returned %v, want %v", err, errHandle)
		}
	}
	if calls != 2 {
		t.Errorf("handler was called %d times, want 2", calls)
	}
}

// Stop ends a race without a winner: a running handler is cancelled and returns nil, and a later one skips the message.
func TestRace_Stop(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		first := middleware.FirstN(1, raceOf)
		msg := job{race: middleware.NewRace()}

		running := make(chan struct{})
		stopped := make(chan error, 1)
		go func() {
			stopped <- first(func(ctx context.Context, _ uint64, _ job) error {
				close(running)
				<-ctx.Done()

				return ctx.Err()
			})(t.Context(), 1, msg)
		}()
		<-running
		msg.race.Stop()
		if err := <-stopped; err != nil {
			t.Errorf("handler running when stopped returned %v, want nil", err)
		}
		if !done(msg.race) {
			t.Error("Done is open once stopped")
		}

		calls := 0
		late := first(func(context.Context, uint64, job) error {
			calls++

			return nil
		})
		if err := late(t.Context(), 2, msg); err != nil || calls != 0 {
			t.Errorf("once stopped, a handler returned %v after %d calls, want nil after none", err, calls)
		}
		if winners := msg.race.Winners(); len(winners) != 0 {
			t.Errorf("Winners returned %v, want none", winners)
		}
	})
}

// Stop keeps the race's winners, and a handler running then does not join them.
func TestRace_StopWinners(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		first := middleware.FirstN(2, raceOf)
		msg := job{race: middleware.NewRace()}

		finish := running(t, first, 2, msg)
		if err := first(func(context.Context, uint64, job) error { return nil })(t.Context(), 1, msg); err != nil {
			t.Errorf("winner returned %v, want nil", err)
		}
		msg.race.Stop()
		if err := finish(); err != nil {
			t.Errorf("handler running when stopped returned %v, want nil", err)
		}
		if winners := msg.race.Winners(); !slices.Equal(winners, []uint64{1}) {
			t.Errorf("Winners returned %v, want [1]", winners)
		}
	})
}

// A handler ignoring its ctx and returning nil once the race is over does not win.
func TestFirstN_Late(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		first := middleware.FirstN(1, raceOf)
		msg := job{race: middleware.NewRace()}

		finish := running(t, first, 2, msg)
		if err := first(func(context.Context, uint64, job) error { return nil })(t.Context(), 1, msg); err != nil {
			t.Errorf("winner returned %v, want nil", err)
		}
		if err := finish(); err != nil {
			t.Errorf("late handler returned %v, want nil", err)
		}
		if winners := msg.race.Winners(); !slices.Equal(winners, []uint64{1}) {
			t.Errorf("Winners returned %v, want [1]", winners)
		}
	})
}

// Won tells a winner from a subscriber that returned nil too late, or took no part.
func TestRace_Won(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		first := middleware.FirstN(1, raceOf)
		msg := job{race: middleware.NewRace()}

		finish := running(t, first, 2, msg)
		if msg.race.Won(1) {
			t.Error("Won reports a winner before any handler returned")
		}
		if err := first(func(context.Context, uint64, job) error { return nil })(t.Context(), 1, msg); err != nil {
			t.Errorf("winner returned %v, want nil", err)
		}
		if err := finish(); err != nil {
			t.Errorf("late handler returned %v, want nil", err)
		}
		for id, want := range map[uint64]bool{1: true, 2: false, 3: false} {
			if got := msg.race.Won(id); got != want {
				t.Errorf("Won(%d) returned %t, want %t", id, got, want)
			}
		}
	})
}

// A race ending between FirstN's check and the handler joining makes it skip the message.
func TestFirstN_EndsWhileJoining(t *testing.T) {
	t.Parallel()

	msg := job{race: middleware.NewRace()}
	calls := 0
	handle := middleware.FirstN(1, raceOf)(func(context.Context, uint64, job) error {
		calls++

		return nil
	})
	if err := handle(stopping{Context: t.Context(), race: msg.race}, 1, msg); err != nil || calls != 0 {
		t.Errorf("a handler returned %v after %d calls, want nil after none", err, calls)
	}
	if winners := msg.race.Winners(); len(winners) != 0 {
		t.Errorf("Winners returned %v, want none", winners)
	}
}

// stopping is a ctx stopping race on Done, which FirstN's context.WithCancel calls between the two.
type stopping struct {
	context.Context //nolint:containedctx // wrapped, to be passed on

	race *middleware.Race
}

func (s stopping) Done() <-chan struct{} {
	s.race.Stop()

	return s.Context.Done()
}

// However many handlers return nil at once, exactly n win, each once. In real time, which is what makes them race.
func TestFirstN_Concurrent(t *testing.T) {
	t.Parallel()

	const n, handlers = 3, 32
	first := middleware.FirstN(n, raceOf)
	for range 100 {
		msg := job{race: middleware.NewRace()}
		// So that all are past FirstN's first check before any returns.
		var inside sync.WaitGroup
		inside.Add(handlers)
		handle := first(func(context.Context, uint64, job) error {
			inside.Done()
			inside.Wait()

			return nil
		})

		var wg sync.WaitGroup
		for id := range uint64(handlers) {
			wg.Go(func() {
				if err := handle(t.Context(), id, msg); err != nil {
					t.Errorf("handler %d returned %v, want nil", id, err)
				}
			})
		}
		wg.Wait()

		winners := msg.race.Winners()
		if distinct := len(slices.Compact(slices.Sorted(slices.Values(winners)))); len(winners) != n || distinct != n {
			t.Fatalf("Winners returned %v, want %d different ones", winners, n)
		}
		if !done(msg.race) {
			t.Fatal("Done is open once the race has its winners")
		}
	}
}

// running starts a handler ignoring its ctx. The func returned makes it return nil, and returns what FirstN made of
// that.
func running(t *testing.T, first subscriber.Middleware[job], id uint64, msg job) func() error {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- first(func(context.Context, uint64, job) error {
			close(started)
			<-release

			return nil
		})(t.Context(), id, msg)
	}()
	<-started

	return func() error {
		close(release)

		return <-result
	}
}

// done reports whether r's Done is closed.
func done(r *middleware.Race) bool {
	select {
	case <-r.Done():
		return true
	default:
		return false
	}
}
