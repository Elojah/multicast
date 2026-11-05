package multicast_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/subscriber"
)

// Once its ctx is done, a subscriber is unsubscribed: Publish no longer reaches it, and its goroutine ends.
func TestSubscribe_ContextDone(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		g := newGroup[int](t)
		handled := &recorder[int]{}
		id, err := g.Subscribe(ctx, handled.handle)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		g.Publish(t.Context(), 1)
		cancel()
		synctest.Wait()

		if n := g.Publish(t.Context(), 2); n != 0 {
			t.Errorf("Publish once ctx is done handed the message to %d subscribers, want 0", n)
		}
		if err := g.Unsubscribe(t.Context(), id); !isNotFound(err) {
			t.Errorf("Unsubscribe once ctx is done = %v, want a *SubscriberNotFoundError", err)
		}
		synctest.Wait()
		if got, want := handled.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
	})
}

// A subscriber whose ctx is already done when it subscribes is unsubscribed right after.
func TestSubscribe_ContextDoneFirst(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		g := newGroup[int](t)
		handled := &recorder[int]{}
		if _, err := g.Subscribe(ctx, handled.handle); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		synctest.Wait()

		if n := g.Publish(t.Context(), 1); n != 0 {
			t.Errorf("Publish handed the message to %d subscribers, want 0", n)
		}
		synctest.Wait()
		if got := handled.messages(); len(got) != 0 {
			t.Errorf("handle got %v, want nothing", got)
		}
	})
}

// A subscriber unsubscribed another way leaves nothing waiting on its ctx.
func TestSubscribe_ContextNoLeak(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		close func(t *testing.T, g *multicast.Group[int], id uint64)
	}{
		{"Unsubscribe", func(t *testing.T, g *multicast.Group[int], id uint64) {
			t.Helper()
			unsubscribe(t, g, id)
		}},
		{"Close", func(t *testing.T, g *multicast.Group[int], _ uint64) {
			t.Helper()
			if err := g.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx := neverDone{Context: context.Background(), done: make(chan struct{})}
				g := newGroup[int](t)
				handled := &recorder[int]{}
				id, err := g.Subscribe(ctx, handled.handle)
				if err != nil {
					t.Fatalf("Subscribe: %v", err)
				}

				g.Publish(t.Context(), 1)
				tt.close(t, g, id)
				synctest.Wait()

				if got, want := handled.messages(), []int{1}; !slices.Equal(got, want) {
					t.Errorf("handle got %v, want %v", got, want)
				}
			})
		})
	}
}

// subscriber.WithDetachedContext keeps the subscriber once its ctx is done, and handle gets a ctx with the same values,
// never done.
func TestSubscriberWithDetachedContext(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		ctx, cancel := context.WithCancel(context.WithValue(t.Context(), key{}, "subscribe"))
		g := newGroup[int](t)
		handled := &recorder[int]{}
		id, err := g.Subscribe(ctx, func(ctx context.Context, id uint64, msg int) error {
			if err := ctx.Err(); err != nil {
				t.Errorf("handle got a done ctx: %v", err)
			}
			if got, want := ctx.Value(key{}), "subscribe"; got != want {
				t.Errorf("handle got ctx value %v, want %v", got, want)
			}

			return handled.handle(ctx, id, msg)
		}, subscriber.WithDetachedContext[int]())
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		cancel()
		synctest.Wait()

		if n := g.Publish(t.Context(), 1); n != 1 {
			t.Errorf("Publish once ctx is done handed the message to %d subscribers, want 1", n)
		}
		unsubscribe(t, g, id)
		synctest.Wait()
		if got, want := handled.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
	})
}

// message.WithContext replaces the Subscribe ctx for one message, and a Publish overrides a default one, nil
// included.
func TestMessageWithContext(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		ctx := context.WithValue(subscribeCtx(t), key{}, "subscribe")
		messageCtx := context.WithValue(t.Context(), key{}, "message")
		defaultCtx := context.WithValue(t.Context(), key{}, "default")
		g := newGroup[int](t)
		subscribeRecording := func(options ...subscriber.Option[int]) (*recorder[string], uint64) {
			values := &recorder[string]{}
			id, err := g.Subscribe(ctx, func(ctx context.Context, _ uint64, _ int) error {
				v, _ := ctx.Value(key{}).(string)
				values.record(v)

				return nil
			}, options...)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			return values, id
		}
		plain, plainID := subscribeRecording()
		defaulted, defaultedID := subscribeRecording(
			subscriber.WithDefaultMessageOptions(message.WithContext[int](defaultCtx)))

		g.Publish(t.Context(), 1)
		g.Publish(t.Context(), 2, message.WithContext[int](messageCtx))
		g.Publish(t.Context(), 3, message.WithContext[int](nil)) //nolint:staticcheck // nil is how to override a default
		unsubscribeAll(t, g, plainID, defaultedID)
		synctest.Wait()

		if got, want := plain.messages(), []string{"subscribe", "message", "subscribe"}; !slices.Equal(got, want) {
			t.Errorf("handle got ctx values %v, want %v", got, want)
		}
		if got, want := defaulted.messages(), []string{"default", "message", "subscribe"}; !slices.Equal(got, want) {
			t.Errorf("handle with a default message ctx got ctx values %v, want %v", got, want)
		}
	})
}

// Every error about a message with its own ctx reaches both error handlers with that ctx, whatever the error.
func TestMessageWithContext_ErrorHandlers(t *testing.T) {
	t.Parallel()

	type key struct{}
	for _, tt := range []struct {
		name string
		want error
		// fail makes a subscriber fail on message 1, sent with options, which give it its ctx and error handler.
		fail func(t *testing.T, g *multicast.Group[int], errorHandler subscriber.Option[int], options ...message.Option[int])
	}{
		{"handle", handleError(1), func(t *testing.T, g *multicast.Group[int], errorHandler subscriber.Option[int], options ...message.Option[int]) {
			t.Helper()
			id := subscribe(t, g, func(_ context.Context, _ uint64, msg int) error {
				return handleError(msg)
			}, errorHandler)
			g.Publish(t.Context(), 1, options...)
			unsubscribe(t, g, id)
		}},
		{"Timeout", subscriber.ErrTimeout, func(t *testing.T, g *multicast.Group[int], errorHandler subscriber.Option[int], options ...message.Option[int]) {
			t.Helper()
			stuck := &recorder[int]{hold: make(chan struct{})}
			id := subscribe(t, g, stuck.handle, errorHandler)
			g.Publish(t.Context(), 0) // stuck is now processing 0 and not reading
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			g.Publish(ctx, 1, options...)
			unsubscribe(t, g, id)
			stuck.release()
		}},
		{"Dropped", subscriber.ErrDropped, func(t *testing.T, g *multicast.Group[int], errorHandler subscriber.Option[int], options ...message.Option[int]) {
			t.Helper()
			stuck := &recorder[int]{hold: make(chan struct{})}
			id := subscribe(t, g, stuck.handle, errorHandler,
				subscriber.WithDefaultMessageOptions(message.WithNonBlocking[int]()))
			g.Publish(t.Context(), 0, message.WithSync[int]()) // stuck is now processing 0 and not reading
			g.Publish(t.Context(), 1, options...)
			unsubscribe(t, g, id)
			stuck.release()
		}},
		{"SubscribeSeq", subscriber.ErrClosed, func(t *testing.T, g *multicast.Group[int], errorHandler subscriber.Option[int], options ...message.Option[int]) {
			t.Helper()
			_, seq := subscribeSeq(t, g, subscriber.WithBuffer[int](2), errorHandler)
			g.Publish(t.Context(), 0)
			g.Publish(t.Context(), 1, options...)
			for range seq {
				break // after 0, leaving 1 unyielded
			}
		}},
		{"WithUnsubscribeDiscard", subscriber.ErrClosed, func(t *testing.T, g *multicast.Group[int], errorHandler subscriber.Option[int], options ...message.Option[int]) {
			t.Helper()
			held := &recorder[int]{hold: make(chan struct{})}
			id := subscribe(t, g, held.handle, subscriber.WithBuffer[int](1), errorHandler)
			g.Publish(t.Context(), 0) // held holds on to 0, and 1 fills the buffer
			g.Publish(t.Context(), 1, options...)
			synctest.Wait()
			if err := g.Unsubscribe(t.Context(), id, subscriber.WithUnsubscribeDiscard()); err != nil {
				t.Fatalf("Unsubscribe: %v", err)
			}
			held.release()
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				got := &recorder[string]{}
				errorHandler := func(who string) func(context.Context, error) {
					return func(ctx context.Context, err error) {
						v, _ := ctx.Value(key{}).(string)
						got.record(fmt.Sprintf("%s: %t, ctx %s, done %v", who, errors.Is(err, tt.want), v, ctx.Err()))
					}
				}
				g := newGroup[int](t)
				tt.fail(t, g, subscriber.WithErrorHandler[int](errorHandler("subscriber")),
					message.WithContext[int](context.WithValue(t.Context(), key{}, "message")),
					message.WithErrorHandler[int](errorHandler("message")))
				synctest.Wait()

				want := []string{"subscriber: true, ctx message, done <nil>", "message: true, ctx message, done <nil>"}
				if got := got.messages(); !slices.Equal(got, want) {
					t.Errorf("error handlers got %q, want %q", got, want)
				}
			})
		})
	}
}

// neverDone is a ctx type unknown to package context, so AfterFunc on it starts a goroutine, which leaks unless
// stopped.
type neverDone struct {
	context.Context //nolint:containedctx // it is the ctx itself, not a struct carrying one

	done chan struct{}
}

func (c neverDone) Done() <-chan struct{} {
	return c.done
}

// Once unsubscribed with WithUnsubscribeDiscard, the subscriber reports its buffer instead of handling it, and a
// waiting Publish reports its message at once.
func TestSubscriberWithUnsubscribeDiscard(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		ctx := context.WithValue(subscribeCtx(t), key{}, "subscribe")
		g := newGroup[int](t)
		handled := &recorder[int]{hold: make(chan struct{})}
		closed := &recorder[int]{}
		id, err := g.Subscribe(ctx, handled.handle, subscriber.WithBuffer[int](2),
			subscriber.WithErrorHandler[int](func(ctx context.Context, err error) {
				if v := ctx.Value(key{}); v != "subscribe" {
					t.Errorf("error handler got a ctx with value %v, want the one passed to Subscribe", v)
				}
				recordClosed(t, closed)(ctx, err)
			}))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		// handle holds on to 1, 2 and 3 fill the buffer, and the Publish of 4 waits.
		for msg := 1; msg <= 3; msg++ {
			g.Publish(t.Context(), msg)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			g.Publish(t.Context(), 4)
		}()
		synctest.Wait()

		if err := g.Unsubscribe(t.Context(), id, subscriber.WithUnsubscribeDiscard()); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		waitClosed(t, done, "Publish to a subscriber unsubscribed with WithUnsubscribeDiscard")
		handled.release()
		synctest.Wait()

		if got, want := handled.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
		if got, want := closed.messages(), []int{4, 2, 3}; !slices.Equal(got, want) {
			t.Errorf("error handler got *subscriber.ClosedError for messages %v, want %v", got, want)
		}
	})
}

// A subscriber's default unsubscribe options apply to Close, and an Unsubscribe's own options override them.
func TestSubscriberWithUnsubscribeOptions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		subscribeHeld := func(closed *recorder[int]) (*recorder[int], uint64) {
			handled := &recorder[int]{hold: make(chan struct{})}
			id := subscribe(t, g, handled.handle, subscriber.WithBuffer[int](2),
				subscriber.WithErrorHandler[int](recordClosed(t, closed)),
				subscriber.WithUnsubscribeOptions[int](subscriber.WithUnsubscribeDiscard()))

			return handled, id
		}
		deliveredClosed := &recorder[int]{}
		deliveredHandled, deliveredID := subscribeHeld(deliveredClosed)
		discardedClosed := &recorder[int]{}
		discardedHandled, _ := subscribeHeld(discardedClosed)

		// Each subscriber holds on to 1, with 2 and 3 in its buffer.
		for msg := 1; msg <= 3; msg++ {
			g.Publish(t.Context(), msg)
		}
		synctest.Wait()
		if err := g.Unsubscribe(t.Context(), deliveredID, subscriber.WithUnsubscribeDeliver()); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		deliveredHandled.release()
		discardedHandled.release()
		synctest.Wait()

		if got, want := deliveredHandled.messages(), []int{1, 2, 3}; !slices.Equal(got, want) {
			t.Errorf("subscriber unsubscribed with WithUnsubscribeDeliver handled %v, want %v", got, want)
		}
		if got := deliveredClosed.messages(); len(got) != 0 {
			t.Errorf("subscriber unsubscribed with WithUnsubscribeDeliver reported %v, want nothing", got)
		}
		if got, want := discardedHandled.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("subscriber discarding by default handled %v after Close, want %v", got, want)
		}
		if got, want := discardedClosed.messages(), []int{2, 3}; !slices.Equal(got, want) {
			t.Errorf("subscriber discarding by default reported %v after Close, want %v", got, want)
		}
	})
}

// After WithUnsubscribeDiscard, a SubscribeSeq loop yields nothing more, and everything taken is reported, after the
// message of a waiting Publish, which gives up at once.
func TestSubscribeSeq_UnsubscribeDiscard(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		closed := &recorder[int]{}
		id, seq := subscribeSeq(t, g, subscriber.WithBuffer[int](2),
			subscriber.WithErrorHandler[int](recordClosed(t, closed)))

		// Nothing reads before the loop starts: 1 and 2 fill the buffer, and the Publish of 3 waits.
		g.Publish(t.Context(), 1)
		g.Publish(t.Context(), 2)
		done := make(chan struct{})
		go func() {
			defer close(done)
			g.Publish(t.Context(), 3)
		}()
		synctest.Wait()

		if err := g.Unsubscribe(t.Context(), id, subscriber.WithUnsubscribeDiscard()); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		waitClosed(t, done, "Publish to a subscriber unsubscribed with WithUnsubscribeDiscard")
		for msg := range seq {
			t.Errorf("loop got %d, want nothing", msg)
		}
		synctest.Wait()

		if got, want := closed.messages(), []int{3, 1, 2}; !slices.Equal(got, want) {
			t.Errorf("error handler got *subscriber.ClosedError for messages %v, want %v", got, want)
		}
	})
}

// recordClosed returns an error handler that records each *subscriber.ClosedError's message into r, and fails on any
// other error.
func recordClosed(t *testing.T, r *recorder[int]) func(context.Context, error) {
	t.Helper()

	return func(_ context.Context, err error) {
		var closedErr *subscriber.ClosedError[int]
		if !errors.As(err, &closedErr) || !errors.Is(err, subscriber.ErrClosed) {
			t.Errorf("error handler got %v, want a *subscriber.ClosedError", err)

			return
		}
		r.record(closedErr.Message)
	}
}

// done gets the ID once, after handle's last call, however the subscriber was removed: once it has handled its buffer
// by default, or reported it with discard. With room in done, it gets the ID even once its ctx is done.
func TestSubscriberWithDone(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		evict  bool
		remove func(t *testing.T, g *multicast.Group[int], id uint64, cancel context.CancelFunc)
		want   []string
	}{
		{"Unsubscribe", false, func(t *testing.T, g *multicast.Group[int], id uint64, _ context.CancelFunc) {
			t.Helper()
			unsubscribe(t, g, id)
		}, []string{"handled 1", "handled 2"}},
		{"UnsubscribeDiscard", false, func(t *testing.T, g *multicast.Group[int], id uint64, _ context.CancelFunc) {
			t.Helper()
			if err := g.Unsubscribe(t.Context(), id, subscriber.WithUnsubscribeDiscard()); err != nil {
				t.Fatalf("Unsubscribe: %v", err)
			}
		}, []string{"handled 1", "2 closed"}},
		{"Close", false, func(t *testing.T, g *multicast.Group[int], _ uint64, _ context.CancelFunc) {
			t.Helper()
			if err := g.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}, []string{"handled 1", "handled 2"}},
		{"ContextDone", false, func(t *testing.T, _ *multicast.Group[int], _ uint64, cancel context.CancelFunc) {
			t.Helper()
			cancel()
		}, []string{"handled 1", "handled 2"}},
		// 3 finds the buffer full and evicts the subscriber, which then discards 2.
		{"Evicted", true, func(t *testing.T, g *multicast.Group[int], _ uint64, _ context.CancelFunc) {
			t.Helper()
			g.Publish(t.Context(), 3)
		}, []string{"handled 1", "3 evicted, timed out", "2 closed"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup[int](t)
				events := &recorder[string]{}
				hold := make(chan struct{})
				ctx, cancel := context.WithCancel(subscribeCtx(t))
				defer cancel()
				// Room for two, so that a second send would show.
				dones := []chan uint64{make(chan uint64, 2), make(chan uint64, 2)}
				options := []subscriber.Option[int]{
					subscriber.WithBuffer[int](1),
					subscriber.WithErrorHandler[int](recordLosses(t, events)),
					subscriber.WithDone[int](ctx, dones[0]),
					subscriber.WithDone[int](ctx, nil),
					subscriber.WithDone[int](ctx, dones[1]),
				}
				if tt.evict {
					options = append(options, subscriber.WithEvict[int](isTimeout), subscriber.WithTimeout[int](time.Second))
				}
				id, err := g.Subscribe(ctx, func(_ context.Context, _ uint64, msg int) error {
					events.record(fmt.Sprintf("handled %d", msg))
					<-hold

					return nil
				}, options...)
				if err != nil {
					t.Fatalf("Subscribe: %v", err)
				}

				// handle holds on to 1, and 2 fills the buffer.
				g.Publish(t.Context(), 1)
				g.Publish(t.Context(), 2)
				synctest.Wait()
				tt.remove(t, g, id, cancel)
				synctest.Wait()
				for _, done := range dones {
					if len(done) != 0 {
						t.Fatalf("done got the ID while handle held on to 1: %q", events.messages())
					}
				}

				close(hold)
				synctest.Wait()
				if got := events.messages(); !slices.Equal(got, tt.want) {
					t.Errorf("got %q, want %q", got, tt.want)
				}
				for _, done := range dones {
					if got, want := received(done), []uint64{id}; !slices.Equal(got, want) {
						t.Errorf("done got %v, want %v", got, want)
					}
				}
			})
		})
	}
}

// Shutdown returns once done has received the ID, or once WithDone's ctx is done: until then, it waits for a receiver.
func TestSubscriberWithDone_Shutdown(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		room int
		// end frees a Shutdown waiting on the send, and returns what done got.
		end  func(done <-chan uint64, cancel context.CancelFunc) []uint64
		want bool
	}{
		{"Room", 1, nil, true},
		{"Receiver", 0, func(done <-chan uint64, _ context.CancelFunc) []uint64 {
			return []uint64{<-done}
		}, true},
		{"ContextDone", 0, func(_ <-chan uint64, cancel context.CancelFunc) []uint64 {
			cancel()

			return nil
		}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup[int](t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan uint64, tt.room)
				id := subscribe(t, g, func(context.Context, uint64, int) error { return nil }, subscriber.WithDone[int](ctx, done))

				errs := shutdown(t.Context(), g)
				var got []uint64
				if tt.end != nil {
					checkWaiting(t, errs, "nothing received the ID")
					got = tt.end(done, cancel)
				}
				if err := waitShutdown(t, errs); err != nil {
					t.Fatalf("Shutdown = %v, want nil", err)
				}
				// Without synctest.Wait: Shutdown returning is enough.
				got = append(got, received(done)...)
				var want []uint64
				if tt.want {
					want = []uint64{id}
				}
				if !slices.Equal(got, want) {
					t.Errorf("done got %v once Shutdown returned, want %v", got, want)
				}
			})
		})
	}
}

// For SubscribeSeq, done gets the ID after the loop body's last call, however the loop ended, and not for a loop until
// it is ranged.
func TestSubscriberWithDone_SubscribeSeq(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		run  func(t *testing.T, g *multicast.Group[int], id uint64, seq iter.Seq2[int, func(error)], done <-chan uint64, events *recorder[string])
		want []string
	}{
		{"Break", func(t *testing.T, g *multicast.Group[int], _ uint64, seq iter.Seq2[int, func(error)], done <-chan uint64, events *recorder[string]) {
			t.Helper()
			go func() {
				for msg := range seq {
					events.record(fmt.Sprintf("yielded %d, %d done", msg, len(done)))

					break
				}
			}()
			g.Publish(t.Context(), 1)
		}, []string{"yielded 1, 0 done"}},
		{"Unsubscribe", func(t *testing.T, g *multicast.Group[int], id uint64, seq iter.Seq2[int, func(error)], done <-chan uint64, events *recorder[string]) {
			t.Helper()
			go func() {
				for msg := range seq {
					events.record(fmt.Sprintf("yielded %d, %d done", msg, len(done)))
				}
			}()
			g.Publish(t.Context(), 1)
			unsubscribe(t, g, id)
		}, []string{"yielded 1, 0 done"}},
		{"NeverRanged", func(t *testing.T, g *multicast.Group[int], id uint64, _ iter.Seq2[int, func(error)], _ <-chan uint64, _ *recorder[string]) {
			t.Helper()
			unsubscribe(t, g, id)
		}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup[int](t)
				events := &recorder[string]{}
				done := make(chan uint64, 1)
				id, seq := subscribeSeq(t, g, subscriber.WithDone[int](t.Context(), done))
				tt.run(t, g, id, seq, done, events)
				synctest.Wait()
				if got := events.messages(); !slices.Equal(got, tt.want) {
					t.Errorf("got %q, want %q", got, tt.want)
				}
				var want []uint64
				if tt.want != nil {
					want = []uint64{id}
				}
				if got := received(done); !slices.Equal(got, want) {
					t.Errorf("done got %v, want %v", got, want)
				}
				// Else the Group waits for it until the bubble ends. Unsubscribed, it ends at once.
				for range seq {
				}
			})
		})
	}
}

// Shutdown returns once every subscriber has handled what it took: the message in handle, its buffer, and what a
// SubscribeSeq loop took. Later calls return ErrClosed.
func TestShutdown(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		release := make(chan struct{})
		handled := &recorder[int]{hold: release}
		subscribe(t, g, handled.handle, subscriber.WithBuffer[int](2))
		_, seq := subscribeSeq(t, g, subscriber.WithBuffer[int](2))
		yielded := &recorder[int]{}
		go func() {
			for msg := range seq {
				yielded.record(msg)
				<-release
			}
		}()

		// handle and the loop hold on to 1, with 2 and 3 in their buffers.
		for msg := 1; msg <= 3; msg++ {
			g.Publish(t.Context(), msg)
		}
		errs := shutdown(t.Context(), g)
		checkWaiting(t, errs, "handle and the loop held message 1")
		close(release)
		if err := waitShutdown(t, errs); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}

		// Without synctest.Wait: Shutdown returning is enough.
		for name, r := range map[string]*recorder[int]{"Subscribe": handled, "SubscribeSeq": yielded} {
			if got, want := r.messages(), []int{1, 2, 3}; !slices.Equal(got, want) {
				t.Errorf("%s subscriber got %v once Shutdown returned, want %v", name, got, want)
			}
		}
		if err := g.Shutdown(t.Context()); !errors.Is(err, multicast.ErrClosed) {
			t.Errorf("second Shutdown = %v, want ErrClosed", err)
		}
		if err := g.Close(); !errors.Is(err, multicast.ErrClosed) {
			t.Errorf("Close after Shutdown = %v, want ErrClosed", err)
		}
		if _, err := g.Subscribe(t.Context(), handled.handle); !errors.Is(err, multicast.ErrClosed) {
			t.Errorf("Subscribe after Shutdown = %v, want ErrClosed", err)
		}
	})
}

// Shutdown frees the Publish calls waiting on a subscriber, sync and async, and returns once they have reported and
// handle has returned.
func TestShutdown_FreesPublish(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		handled := &recorder[int]{hold: make(chan struct{})}
		closed := &recorder[int]{}
		subscribe(t, g, handled.handle, subscriber.WithErrorHandler[int](recordClosed(t, closed)))

		g.Publish(t.Context(), 1) // handle holds on to it
		done := make(chan struct{})
		go func() {
			defer close(done)
			g.Publish(t.Context(), 2)
		}()
		g.Publish(t.Context(), 3, message.WithAsync[int]())
		synctest.Wait() // both sends wait on the subscriber

		errs := shutdown(t.Context(), g)
		waitClosed(t, done, "Publish of 2 once shut down")
		checkWaiting(t, errs, "handle held message 1")
		handled.release()
		if err := waitShutdown(t, errs); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}

		if got, want := handled.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
		// Both sends gave up at once, in any order.
		if got, want := slices.Sorted(slices.Values(closed.messages())), []int{2, 3}; !slices.Equal(got, want) {
			t.Errorf("error handler got *subscriber.ClosedError for messages %v once Shutdown returned, want %v", got, want)
		}
	})
}

// Shutdown gives up once its ctx is done, here while handle hangs. A later call waits again.
func TestShutdown_ContextDone(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		handled, _ := hold(t, g)
		g.Publish(t.Context(), 1)

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := g.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Shutdown while handle hangs = %v, want %v", err, context.DeadlineExceeded)
		}

		errs := shutdown(t.Context(), g)
		checkWaiting(t, errs, "handle held message 1")
		handled.release()
		if err := waitShutdown(t, errs); !errors.Is(err, multicast.ErrClosed) {
			t.Errorf("second Shutdown = %v, want ErrClosed", err)
		}
	})
}

// A SubscribeSeq loop never started holds Shutdown until its ctx is done, since it still has a message to yield. Once
// ranged, the loop yields it and ends, and Shutdown returns.
func TestShutdown_SubscribeSeqNotRanged(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		_, seq := subscribeSeq(t, g, subscriber.WithBuffer[int](1))
		g.Publish(t.Context(), 1) // fills the buffer

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := g.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Shutdown before the loop started = %v, want %v", err, context.DeadlineExceeded)
		}

		yielded := &recorder[int]{}
		for msg := range seq {
			yielded.record(msg)
		}
		if err := waitShutdown(t, shutdown(t.Context(), g)); !errors.Is(err, multicast.ErrClosed) {
			t.Errorf("Shutdown once the loop ended = %v, want ErrClosed", err)
		}
		if got, want := yielded.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("loop got %v, want %v", got, want)
		}
	})
}

// shutdown calls g.Shutdown in a goroutine, and returns a channel that yields its error.
func shutdown[T any](ctx context.Context, g *multicast.Group[T]) <-chan error {
	errs := make(chan error, 1)
	go func() { errs <- g.Shutdown(ctx) }()

	return errs
}

// waitShutdown returns the error errs yields, or fails after deadlockTimeout.
func waitShutdown(t *testing.T, errs <-chan error) error {
	t.Helper()
	select {
	case err := <-errs:
		return err
	case <-time.After(deadlockTimeout):
		t.Fatalf("still waiting for Shutdown after %v: deadlock", deadlockTimeout)

		return nil
	}
}

// checkWaiting fails the test if errs yielded already, once every goroutine of the bubble is blocked.
func checkWaiting(t *testing.T, errs <-chan error, while string) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-errs:
		t.Fatalf("Shutdown returned %v while %s", err, while)
	default:
	}
}
