package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/middleware"
	"github.com/elojah/multicast/reply"
	replyholder "github.com/elojah/multicast/reply/holder"
	replymiddleware "github.com/elojah/multicast/reply/middleware"
	"github.com/elojah/multicast/subscriber"
)

// deadlockTimeout bounds every wait, so a deadlock fails instead of hanging. In a synctest bubble, it fires exactly
// when the bubble deadlocks.
const deadlockTimeout = 10 * time.Second

var errOffline = errors.New("sensor offline")

// ask is the request every test publishes: a query, answered with an int.
type ask = reply.Request[string, int]

// results is what Publish returns.
type results = map[uint64]reply.Result[int]

// handleFunc is what Group.Subscribe takes.
type handleFunc = func(ctx context.Context, id uint64, query string) (int, error)

// Publish returns the winners and those that failed before the race ended, once the losers have stopped, wherever
// FirstN sits. A Retry inside FirstN is still waiting when the race ends, so the one failing loses too.
func TestFirstN(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		retry bool
	}{
		{name: "alone"},
		{name: "retry", retry: true},
	} {
		for _, byDefault := range []bool{false, true} {
			t.Run(tt.name+"/"+placement(byDefault), func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					race := middleware.NewRace()
					g, options := groupWith(t, byDefault, func() []subscriber.Option[ask] {
						middlewares := []subscriber.Middleware[ask]{racing(2, raceOf(race))}
						if tt.retry {
							middlewares = append(middlewares, middleware.Retry[ask](middleware.RetryPolicy{Attempts: 3, Delay: time.Hour}))
						}

						return []subscriber.Option[ask]{subscriber.WithMiddleware(middlewares...)}
					})
					// A value along with its error, which no answer may carry.
					subscribe(t, g, func(ctx context.Context, _ uint64, _ string) (int, error) {
						<-ctx.Done()

						return 7, ctx.Err()
					}, options...)
					failed := subscribe(t, g, failing, options...)
					first := subscribe(t, g, slow(time.Second, 5), options...)
					second := subscribe(t, g, slow(2*time.Second, 6), options...)

					want := results{first: {Value: 5}, second: {Value: 6}}
					if !tt.retry {
						want[failed] = reply.Result[int]{Err: errOffline}
					}
					start := time.Now()
					equal(t, publish(t, g, "north"), want)
					if elapsed := time.Since(start); elapsed != 2*time.Second {
						t.Errorf("Publish returned after %v, want %v", elapsed, 2*time.Second)
					}
					if winners := race.Winners(); !slices.Equal(winners, []uint64{first, second}) {
						t.Errorf("Winners returned %v, want [%d %d]", winners, first, second)
					}
				})
			})
		}
	}
}

// A loser ignoring its ctx holds Publish up but stays out of it, even from the default options, where it answers before
// FirstN knows it lost.
func TestFirstN_Late(t *testing.T) {
	t.Parallel()

	for _, byDefault := range []bool{false, true} {
		t.Run(placement(byDefault), func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				race := middleware.NewRace()
				g, options := groupWith(t, byDefault, func() []subscriber.Option[ask] {
					return []subscriber.Option[ask]{subscriber.WithMiddleware(racing(1, raceOf(race)))}
				})
				winner := subscribe(t, g, slow(time.Second, 5), options...)
				subscribe(t, g, slow(2*time.Second, 7), options...)

				start := time.Now()
				equal(t, publish(t, g, "north"), results{winner: {Value: 5}})
				if elapsed := time.Since(start); elapsed != 2*time.Second {
					t.Errorf("Publish returned after %v, want %v", elapsed, 2*time.Second)
				}
			})
		})
	}
}

// A SubscribeSeq loop that loses leaves Publish like a handle, having failed the request or answered it.
func TestFirstN_SubscribeSeq(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		body func(id uint64, req ask, fail func(error))
	}{
		{name: "fail", body: func(_ uint64, _ ask, fail func(error)) { fail(errOffline) }},
		{name: "answer", body: func(id uint64, req ask, _ func(error)) { req.Answer(id, 7, nil) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				race := middleware.NewRace()
				option := subscriber.WithMiddleware(racing(1, raceOf(race)))
				g := newGroup(t)
				id, seq, err := g.SubscribeSeq(subscribeCtx(t), option)
				if err != nil {
					t.Fatalf("SubscribeSeq: %v", err)
				}
				go func() {
					for req, fail := range seq {
						<-race.Done()
						tt.body(id, req, fail)
					}
				}()
				winner := subscribe(t, g, slow(time.Second, 5), option)

				start := time.Now()
				equal(t, publish(t, g, "north"), results{winner: {Value: 5}})
				if elapsed := time.Since(start); elapsed != time.Second {
					t.Errorf("Publish returned after %v, want %v", elapsed, time.Second)
				}
			})
		})
	}
}

// A request whose race is over leaves Publish without reaching handle or a loop.
func TestFirstN_Over(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		race := middleware.NewRace()
		race.Stop()
		option := subscriber.WithMiddleware(racing(1, raceOf(race)))
		g := newGroup(t)
		subscribe(t, g, func(context.Context, uint64, string) (int, error) {
			t.Error("handle got a request whose race is over")

			return 0, nil
		}, option)
		_, seq, err := g.SubscribeSeq(subscribeCtx(t), option)
		if err != nil {
			t.Fatalf("SubscribeSeq: %v", err)
		}
		go func() {
			for req := range seq {
				t.Errorf("loop got %v, whose race is over", req.Query)
			}
		}()

		start := time.Now()
		equal(t, publish(t, g, "north"), results{})
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Publish returned after %v, want at once", elapsed)
		}
	})
}

// A request merged into one that loses leaves its Publish too.
func TestFirstN_Merged(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		race := middleware.NewRace()
		race.Stop()
		g := newGroup(t)
		subscribe(t, g, answering, subscriber.WithHolder[ask](replyholder.NewLatest[string, int](time.Second)),
			subscriber.WithMiddleware(racing(1, raceOf(race))))

		start := time.Now()
		var wg sync.WaitGroup
		for _, query := range []string{"north", "south"} {
			wg.Go(func() {
				equal(t, publish(t, g, query), results{})
				if elapsed := time.Since(start); elapsed != time.Second {
					t.Errorf("Publish of %s returned after %v, want %v", query, elapsed, time.Second)
				}
			})
			// Once the subscriber holds the request.
			synctest.Wait()
		}
		wg.Wait()
	})
}

// Race.Stop ends a Retry inside FirstN that outlives its Publish, so that it holds up no later one.
func TestFirstN_Stop(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		race := middleware.NewRace()
		g := newGroup(t)
		id := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) {
			if query == "north" {
				return 0, errOffline
			}

			return len(query), nil
		}, subscriber.WithMiddleware(
			racing(1, func(query string) *middleware.Race {
				if query == "north" {
					return race
				}

				return nil
			}),
			middleware.Retry[ask](middleware.RetryPolicy{Attempts: 3, Delay: time.Hour}),
		))

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		equal(t, g.Publish(ctx, "north"), results{id: {Err: subscriber.ErrTimeout}})
		race.Stop()

		start := time.Now()
		equal(t, publish(t, g, "south"), results{id: {Value: 5}})
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("next Publish returned after %v, want at once", elapsed)
		}
	})
}

// groupWith returns a Group, and the options to Subscribe with, giving its subscribers options from
// WithDefaultSubscriberOptions if byDefault, else from Subscribe.
func groupWith(
	t *testing.T, byDefault bool, options func() []subscriber.Option[ask],
) (*reply.Group[string, int], []subscriber.Option[ask]) {
	t.Helper()
	if byDefault {
		return newGroup(t, multicast.WithDefaultSubscriberOptions(options)), nil
	}

	return newGroup(t), options()
}

func placement(byDefault bool) string {
	if byDefault {
		return "default"
	}

	return "subscribe"
}

// newGroup returns a Group closed in Cleanup, since its goroutines would otherwise outlive a synctest bubble.
func newGroup(t *testing.T, options ...multicast.Option[ask]) *reply.Group[string, int] {
	t.Helper()
	g := reply.New(options...)
	t.Cleanup(func() { _ = g.Close() })

	return g
}

// subscribeCtx is the test's ctx, never done, so that a leaked subscriber fails the test.
func subscribeCtx(t *testing.T) context.Context {
	t.Helper()

	return context.WithoutCancel(t.Context())
}

func subscribe(t *testing.T, g *reply.Group[string, int], handle handleFunc, options ...subscriber.Option[ask]) uint64 {
	t.Helper()
	id, err := g.Subscribe(subscribeCtx(t), handle, options...)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	return id
}

// publish is Group.Publish, bounded by deadlockTimeout.
func publish(t *testing.T, g *reply.Group[string, int], query string, options ...message.Option[ask]) results {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), deadlockTimeout)
	defer cancel()

	return g.Publish(ctx, query, options...)
}

// equal fails t unless got holds want's answers, errors compared with errors.Is.
func equal(t *testing.T, got, want results) {
	t.Helper()
	if !maps.EqualFunc(got, want, func(x, y reply.Result[int]) bool {
		return x.Value == y.Value && errors.Is(x.Err, y.Err)
	}) {
		t.Errorf("Publish returned %s, want %s", describe(got), describe(want))
	}
}

func describe(results results) string {
	s := make([]string, 0, len(results))
	for _, id := range slices.Sorted(maps.Keys(results)) {
		s = append(s, fmt.Sprintf("%d: {%d %v}", id, results[id].Value, results[id].Err))
	}

	return fmt.Sprint(s)
}

func answering(_ context.Context, _ uint64, query string) (int, error) { return len(query), nil }

func failing(context.Context, uint64, string) (int, error) { return 1, errOffline }

// slow returns a handle answering value after d, whatever its ctx.
func slow(d time.Duration, value int) handleFunc {
	return func(context.Context, uint64, string) (int, error) {
		time.Sleep(d)

		return value, nil
	}
}

// racing makes a subscriber race for each request race returns a Race for.
func racing(n int, race func(query string) *middleware.Race) subscriber.Middleware[ask] {
	return replymiddleware.FirstN[string, int](n, race)
}

// raceOf returns what gives every request r.
func raceOf(r *middleware.Race) func(string) *middleware.Race {
	return func(string) *middleware.Race { return r }
}
