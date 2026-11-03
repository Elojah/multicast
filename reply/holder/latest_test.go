package holder_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast/reply"
	"github.com/elojah/multicast/reply/holder"
	"github.com/elojah/multicast/subscriber"
)

// deadlockTimeout bounds every wait, so a deadlock fails instead of hanging. In a synctest bubble, it fires exactly
// when the bubble deadlocks.
const deadlockTimeout = 10 * time.Second

// Of the requests a window takes, the last one is handled, and answers every one, including those merged into a
// request merged since.
func TestLatest(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := reply.New[string, int]()
		t.Cleanup(func() { _ = g.Close() })
		var queries []string // Only the subscriber's goroutine appends, before answering.
		// Never done, so that a leaked subscriber fails the test.
		id, err := g.Subscribe(context.WithoutCancel(t.Context()), func(_ context.Context, _ uint64, query string) (int, error) {
			queries = append(queries, query)

			return len(query), nil
		}, subscriber.WithHolder(holder.NewLatest[string, int](time.Second)))
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		publish := func(query string) map[uint64]reply.Result[int] {
			ctx, cancel := context.WithTimeout(t.Context(), deadlockTimeout)
			defer cancel()

			return g.Publish(ctx, query)
		}
		want := map[uint64]reply.Result[int]{id: {Value: len("south-west")}}

		merged := make(chan map[uint64]reply.Result[int], 2)
		for _, query := range []string{"north", "south"} {
			go func() { merged <- publish(query) }()
			// Once the subscriber holds the request.
			synctest.Wait()
		}
		start := time.Now()
		if got := publish("south-west"); !maps.Equal(got, want) {
			t.Errorf("Publish returned %v, want %v", got, want)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("Publish returned after %v, want once the window ends, after %v", elapsed, time.Second)
		}
		for range 2 {
			if got := <-merged; !maps.Equal(got, want) {
				t.Errorf("Publish of a request merged away returned %v, want %v", got, want)
			}
		}
		if want := []string{"south-west"}; !slices.Equal(queries, want) {
			t.Errorf("handle got %v, want %v", queries, want)
		}
	})
}

// handle's error answers every request merged into the one it failed for, with the *subscriber.HandleError about it.
func TestLatest_Error(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		id := subscribe(t, g, func(context.Context, uint64, string) (int, error) { return 0, errOffline },
			subscriber.WithHolder(holder.NewLatest[string, int](time.Second)))

		got, elapsed := publishAll(t, g, "north", "south")()
		for i := range got {
			var handleErr *subscriber.HandleError[ask]
			if err := got[i][id].Err; !errors.As(err, &handleErr) || handleErr.Message.Query != "south" || !errors.Is(err, errOffline) {
				t.Errorf("Publish %d answered %v, want the *HandleError about south, wrapping %v", i, err, errOffline)
			}
			if elapsed[i] != time.Second {
				t.Errorf("Publish %d returned after %v, want %v", i, elapsed[i], time.Second)
			}
		}
	})
}

// Unsubscribed, a subscriber handles the request it holds at once, which answers those merged into it, or with
// WithUnsubscribeDiscard answers each one a *ClosedError.
func TestLatest_Unsubscribe(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		options []subscriber.UnsubscribeOption
		want    reply.Result[int]
	}{
		{name: "deliver", want: reply.Result[int]{Value: 5}},
		{name: "discard", options: []subscriber.UnsubscribeOption{subscriber.WithUnsubscribeDiscard()}, want: reply.Result[int]{Err: subscriber.ErrClosed}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup(t)
				id := subscribe(t, g, answering, subscriber.WithHolder(holder.NewLatest[string, int](time.Second)))

				wait := publishAll(t, g, "north", "south")
				if err := g.Unsubscribe(t.Context(), id, tt.options...); err != nil {
					t.Fatalf("Unsubscribe: %v", err)
				}
				got, elapsed := wait()
				for i := range got {
					if result := got[i][id]; result.Value != tt.want.Value || !errors.Is(result.Err, tt.want.Err) {
						t.Errorf("Publish %d answered %+v, want %+v", i, result, tt.want)
					}
					if elapsed[i] != 0 {
						t.Errorf("Publish %d returned after %v, want at once", i, elapsed[i])
					}
				}
			})
		})
	}
}

// A SubscribeSeq loop's answer to the request released answers those merged into it.
func TestLatest_SubscribeSeq(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		id, seq, err := g.SubscribeSeq(context.WithoutCancel(t.Context()), subscriber.WithHolder(holder.NewLatest[string, int](time.Second)))
		if err != nil {
			t.Fatalf("SubscribeSeq: %v", err)
		}
		go func() {
			for req := range seq {
				req.Answer(id, len(req.Query), nil)
			}
		}()

		got, _ := publishAll(t, g, "north", "south-west")()
		want := results{id: {Value: len("south-west")}}
		for i := range got {
			if !maps.Equal(got[i], want) {
				t.Errorf("Publish %d returned %v, want %v", i, got[i], want)
			}
		}
	})
}

// With d <= 0, each request is handled alone, at once.
func TestLatest_NoWindow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		id := subscribe(t, g, answering, subscriber.WithHolder(holder.NewLatest[string, int](0)))

		got, elapsed := publishAll(t, g, "north", "south-west")()
		for i, want := range []results{{id: {Value: 5}}, {id: {Value: 10}}} {
			if !maps.Equal(got[i], want) || elapsed[i] != 0 {
				t.Errorf("Publish %d returned %v after %v, want %v at once", i, got[i], elapsed[i], want)
			}
		}
	})
}

var errOffline = errors.New("sensor offline")

// ask is the request every test publishes.
type ask = reply.Request[string, int]

// results is what Publish returns.
type results = map[uint64]reply.Result[int]

// newGroup returns a Group closed in Cleanup, since its goroutines would otherwise outlive a synctest bubble.
func newGroup(t *testing.T) *reply.Group[string, int] {
	t.Helper()
	g := reply.New[string, int]()
	t.Cleanup(func() { _ = g.Close() })

	return g
}

// subscribe subscribes handle with a ctx never done, so that a leaked subscriber fails the test.
func subscribe(
	t *testing.T, g *reply.Group[string, int], handle func(context.Context, uint64, string) (int, error),
	options ...subscriber.Option[ask],
) uint64 {
	t.Helper()
	id, err := g.Subscribe(context.WithoutCancel(t.Context()), handle, options...)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	return id
}

// publishAll publishes each query from a goroutine of its own, the next once the previous one waits, and returns a
// func waiting for them, which returns what each Publish returned and after how long. They all start at the same
// instant, since synctest.Wait takes no time.
func publishAll(t *testing.T, g *reply.Group[string, int], queries ...string) func() ([]results, []time.Duration) {
	t.Helper()
	got := make([]results, len(queries))
	elapsed := make([]time.Duration, len(queries))
	start := time.Now()
	var wg sync.WaitGroup
	for i, query := range queries {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), deadlockTimeout)
			defer cancel()
			got[i] = g.Publish(ctx, query)
			elapsed[i] = time.Since(start)
		})
		// Once the subscriber holds the request, or has answered it.
		synctest.Wait()
	}

	return func() ([]results, []time.Duration) {
		wg.Wait()

		return got, elapsed
	}
}

func answering(_ context.Context, _ uint64, query string) (int, error) { return len(query), nil }
