package middleware_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast/middleware"
)

// job is a message carrying its race.
type job struct {
	race *middleware.Race
}

func raceOf(j job) *middleware.Race { return j.race }

// The first handler to return nil wins: the ctx of one still handling the message is cancelled, and its error dropped,
// and one yet to handle it skips it.
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

// With n = 2, the race goes on after the first winner, and the second ends it: the ctx of the third is cancelled only
// then.
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

// Stop ends a race without a winner: a handler still running has its ctx cancelled and returns nil, one yet to run
// skips the message, and none wins after.
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

// done reports whether r's Done is closed.
func done(r *middleware.Race) bool {
	select {
	case <-r.Done():
		return true
	default:
		return false
	}
}
