package reply_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast"
	"github.com/elojah/multicast/holder"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/middleware"
	"github.com/elojah/multicast/reply"
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

// Each subscriber that takes a request answers it, with handle's value or its error, which the error handlers get
// first, as a *subscriber.HandleError.
func TestPublish(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		first := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) { return len(query), nil })
		second := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) { return 2 * len(query), nil })
		reported := make(chan error, 1)
		failing := subscribe(t, g, func(context.Context, uint64, string) (int, error) { return 1, errOffline },
			subscriber.WithErrorHandler[ask](func(_ context.Context, err error) { reported <- err }))

		equal(t, publish(t, g, "north"), results{
			first:   {Value: 5},
			second:  {Value: 10},
			failing: {Err: errOffline},
		})

		select {
		case err := <-reported:
			var handleErr *subscriber.HandleError[ask]
			if !errors.As(err, &handleErr) || handleErr.SubscriberID != failing || !errors.Is(err, errOffline) {
				t.Errorf("error handler got %v, want a *HandleError of subscriber %d wrapping %v", err, failing, errOffline)
			}
		default:
			t.Error("error handler had not run once Publish returned")
		}
	})
}

// Publish waits for every subscriber the request reached only until ctx is done, and returns a *TimeoutError for the
// ones that have not answered by then. An answer coming after is dropped, without holding up its subscriber.
func TestPublish_ContextDone(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		quick := subscribe(t, g, func(context.Context, uint64, string) (int, error) { return 1, nil })
		release := make(chan struct{})
		slow := subscribe(t, g, func(context.Context, uint64, string) (int, error) {
			<-release

			return 2, nil
		})

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		got := g.Publish(ctx, "first")
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("Publish returned after %v, want once ctx is done, after %v", elapsed, time.Second)
		}
		equal(t, got, results{quick: {Value: 1}, slow: {Err: subscriber.ErrTimeout}})
		if err := got[slow].Err; !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("subscriber that did not answer got %v, want it to wrap %v", err, context.DeadlineExceeded)
		}

		close(release)
		equal(t, publish(t, g, "second"), results{quick: {Value: 1}, slow: {Value: 2}})
	})
}

// Without a subscriber, Publish returns an empty map at once.
func TestPublish_NoSubscriber(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		start := time.Now()
		if got := publish(t, g, "north"); got == nil || len(got) != 0 || time.Since(start) != 0 {
			t.Errorf("Publish returned %#v after %v, want an empty map at once", got, time.Since(start))
		}
	})
}

// A subscriber whose filter rejects the request, or that the request's subscriber filter skips, is not waited for.
func TestPublish_Filters(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		answer := func(_ context.Context, _ uint64, query string) (int, error) { return len(query), nil }
		north := subscribe(t, g, answer, subscriber.WithFilter(func(req ask) bool { return req.Query == "north" }))
		subscribe(t, g, answer, subscriber.WithFilter(func(req ask) bool { return req.Query == "south" }))
		skipped := subscribe(t, g, answer)
		unfiltered := subscribe(t, g, answer)

		got := publish(t, g, "north", message.WithSubscriberFilter[ask](func(id uint64) bool { return id != skipped }))
		equal(t, got, results{north: {Value: 5}, unfiltered: {Value: 5}})
	})
}

// Publish waits for every subscriber that took the request, whatever the delivery mode.
func TestPublish_Delivery(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name              string
		subscriberOptions []subscriber.Option[ask]
		messageOptions    []message.Option[ask]
	}{
		{name: "sync"},
		{name: "buffered", subscriberOptions: []subscriber.Option[ask]{subscriber.WithBuffer[ask](1)}},
		{name: "parallel", messageOptions: []message.Option[ask]{message.WithParallel[ask]()}},
		{name: "async", messageOptions: []message.Option[ask]{message.WithAsync[ask]()}},
		{
			name:              "non-blocking",
			subscriberOptions: []subscriber.Option[ask]{subscriber.WithBuffer[ask](1)},
			messageOptions:    []message.Option[ask]{message.WithNonBlocking[ask]()},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup(t)
				want := results{}
				for value := range 3 {
					id := subscribe(t, g, func(context.Context, uint64, string) (int, error) {
						return value, nil
					}, tt.subscriberOptions...)
					want[id] = reply.Result[int]{Value: value}
				}

				equal(t, publish(t, g, "north", tt.messageOptions...), want)
			})
		})
	}
}

// A request the library loses, before or after the subscriber takes it, answers the error it reports, so Publish does
// not wait for ctx.
func TestPublish_Lost(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name              string
		subscriberOptions []subscriber.Option[ask]
		messageOptions    []message.Option[ask]
		want              error
		elapsed           time.Duration
	}{
		{
			name:           "non-blocking",
			messageOptions: []message.Option[ask]{message.WithNonBlocking[ask]()},
			want:           subscriber.ErrDropped,
		},
		{
			name:           "async timeout",
			messageOptions: []message.Option[ask]{message.WithAsync[ask](), message.WithTimeout[ask](time.Second)},
			want:           subscriber.ErrTimeout,
			elapsed:        time.Second,
		},
		{
			name:              "holder full",
			subscriberOptions: []subscriber.Option[ask]{subscriber.WithHolder(full{})},
			want:              subscriber.ErrDropped,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup(t)
				release := make(chan struct{})
				defer close(release)
				id := busy(t, g, release, tt.subscriberOptions...)

				start := time.Now()
				equal(t, publish(t, g, "north", tt.messageOptions...), results{id: {Err: tt.want}})
				if elapsed := time.Since(start); elapsed != tt.elapsed {
					t.Errorf("Publish returned after %v, want %v", elapsed, tt.elapsed)
				}
			})
		})
	}
}

// A request an unsubscribe discards answers a *ClosedError.
func TestPublish_Discarded(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		release := make(chan struct{})
		id := busy(t, g, release, subscriber.WithBuffer[ask](1))
		unsubscribed := make(chan struct{})
		go func() {
			defer close(unsubscribed)
			// Once Publish waits for the request it buffered.
			synctest.Wait()
			if err := g.Unsubscribe(t.Context(), id, subscriber.WithUnsubscribeDiscard()); err != nil {
				t.Errorf("Unsubscribe: %v", err)
			}
			close(release)
		}()

		equal(t, publish(t, g, "north"), results{id: {Err: subscriber.ErrClosed}})
		<-unsubscribed
	})
}

// Publish's own error handler runs before the answer.
func TestPublish_ErrorHandler(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		failing := subscribe(t, g, func(context.Context, uint64, string) (int, error) { return 0, errOffline })
		reported := make(chan error, 1)

		got := publish(t, g, "north", message.WithErrorHandler[ask](func(_ context.Context, err error) { reported <- err }))
		equal(t, got, results{failing: {Err: errOffline}})
		select {
		case err := <-reported:
			if !errors.Is(err, errOffline) {
				t.Errorf("error handler got %v, want %v", err, errOffline)
			}
		default:
			t.Error("error handler had not run once Publish returned")
		}
	})
}

// With middleware.Retry, a subscriber answers once, the last attempt's value or error.
func TestSubscribe_Retry(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		retry := subscriber.WithMiddleware(middleware.Retry[ask](middleware.RetryPolicy{Attempts: 3, Delay: time.Second}))
		recovered := subscribe(t, g, recovering(), retry)
		failed := subscribe(t, g, func(context.Context, uint64, string) (int, error) { return 1, errOffline }, retry)

		equal(t, publish(t, g, "north"), results{recovered: {Value: 3}, failed: {Err: errOffline}})
	})
}

// The middlewares from multicast.WithDefaultSubscriberOptions run outside the answer, which still waits for them:
// Retry answers its last attempt, and Recover a panic.
func TestSubscribe_DefaultMiddlewares(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t, multicast.WithDefaultSubscriberOptions(func() []subscriber.Option[ask] {
			return []subscriber.Option[ask]{subscriber.WithMiddleware(
				middleware.Recover[ask](),
				middleware.Retry[ask](middleware.RetryPolicy{Attempts: 3, Delay: time.Second}),
			)}
		}))
		recovered := subscribe(t, g, recovering())
		failed := subscribe(t, g, func(context.Context, uint64, string) (int, error) { return 1, errOffline })
		panicked := subscribe(t, g, func(context.Context, uint64, string) (int, error) { panic(errOffline) })

		start := time.Now()
		equal(t, publish(t, g, "north"), results{
			recovered: {Value: 3},
			failed:    {Err: errOffline},
			panicked:  {Err: subscriber.ErrPanic},
		})
		// Two retries of a second each.
		if elapsed := time.Since(start); elapsed != 2*time.Second {
			t.Errorf("Publish returned after %v, want %v", elapsed, 2*time.Second)
		}
	})
}

// The answer wraps the middlewares passed to Subscribe, so its error is the one they return.
func TestSubscribe_Middlewares(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		middlewares := subscriber.WithMiddleware(middleware.Recover[ask](), middleware.WrapError[ask]())
		panicking := subscribe(t, g, func(context.Context, uint64, string) (int, error) { panic(errOffline) }, middlewares)
		failing := subscribe(t, g, func(context.Context, uint64, string) (int, error) { return 0, errOffline }, middlewares)

		got := publish(t, g, "north")
		var panicErr *subscriber.PanicError[ask]
		if err := got[panicking].Err; !errors.As(err, &panicErr) {
			t.Errorf("subscriber that panicked answered %v, want a *PanicError", err)
		}
		var handleErr *subscriber.HandleError[ask]
		if err := got[failing].Err; !errors.As(err, &handleErr) || !errors.Is(handleErr.Err, errOffline) {
			t.Errorf("subscriber that failed answered %v, want one *HandleError wrapping %v", err, errOffline)
		}
	})
}

// A SubscribeSeq loop answers with Answer, or with fail.
func TestGroup_SubscribeSeq(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		id, seq, err := g.SubscribeSeq(subscribeCtx(t))
		if err != nil {
			t.Fatalf("SubscribeSeq: %v", err)
		}
		go func() {
			for req, fail := range seq {
				if req.Query == "attic" {
					fail(errOffline)
				} else {
					req.Answer(id, len(req.Query), nil)
				}
			}
		}()

		equal(t, publish(t, g, "north"), results{id: {Value: 5}})
		equal(t, publish(t, g, "attic"), results{id: {Err: errOffline}})
	})
}

// A batch handler answers each request of its batch with Answer.
func TestRequest_AnswerBatch(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		id := subscribe(t, g, func(context.Context, uint64, string) (int, error) {
			t.Error("handle got a request of a batch")

			return 0, nil
		}, subscriber.WithHolder(holder.NewBatch[ask](time.Second, 2)),
			subscriber.WithBatchHandler(func(_ context.Context, id uint64, batch message.Batch[ask]) error {
				for req := range batch.All() {
					req.Answer(id, len(req.Query), nil)
				}

				return nil
			}))

		first := make(chan results, 1)
		go func() { first <- publish(t, g, "north") }()
		equal(t, publish(t, g, "south-west"), results{id: {Value: 10}})
		equal(t, <-first, results{id: {Value: 5}})
	})
}

// A request Group.Publish did not send, such as one replayed, or published with the embedded Group's Publish or
// PublishBatch, is handled, its answer dropped.
func TestRequest_AnswerNowhere(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		queries := make(chan string, 4)
		id := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) {
			queries <- query

			return len(query), nil
		}, subscriber.WithReplay(slices.Values([]ask{{Query: "replayed"}})))

		if n := g.Group.Publish(t.Context(), ask{Query: "published"}); n != 1 {
			t.Errorf("multicast.Group.Publish returned %d, want 1", n)
		}
		if n := g.PublishBatch(t.Context(), &message.Slice[ask]{{Query: "batched"}}); n != 1 {
			t.Errorf("PublishBatch returned %d, want 1", n)
		}
		equal(t, publish(t, g, "asked"), results{id: {Value: 5}})

		close(queries)
		var got []string
		for query := range queries {
			got = append(got, query)
		}
		if want := []string{"replayed", "published", "batched", "asked"}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
	})
}

// Hold keeps Publish waiting past the last answer, until Release.
func TestRequest_Hold(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		id, seq, err := g.SubscribeSeq(subscribeCtx(t))
		if err != nil {
			t.Fatalf("SubscribeSeq: %v", err)
		}
		go func() {
			for req := range seq {
				req.Hold()
				req.Answer(id, 5, nil)
				time.Sleep(time.Second)
				req.Release()
			}
		}()

		start := time.Now()
		equal(t, publish(t, g, "north"), results{id: {Value: 5}})
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("Publish returned after %v, want %v", elapsed, time.Second)
		}
	})
}

// Withdraw takes a subscriber out of Publish, whether it has answered or not, and drops any later answer.
func TestRequest_Withdraw(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		body func(id uint64, req ask)
	}{
		{name: "answered", body: func(id uint64, req ask) {
			req.Hold()
			defer req.Release()
			req.Answer(id, 7, nil)
			req.Withdraw(id)
		}},
		{name: "unanswered", body: func(id uint64, req ask) {
			req.Withdraw(id)
			req.Answer(id, 7, nil)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup(t)
				id, seq, err := g.SubscribeSeq(subscribeCtx(t))
				if err != nil {
					t.Fatalf("SubscribeSeq: %v", err)
				}
				go func() {
					for req := range seq {
						tt.body(id, req)
					}
				}()
				kept := subscribe(t, g, func(context.Context, uint64, string) (int, error) { return 5, nil })

				equal(t, publish(t, g, "north"), results{kept: {Value: 5}})
			})
		})
	}
}

// Hold, Release and Withdraw do nothing to a request Group.Publish did not send.
func TestRequest_HoldNowhere(t *testing.T) {
	t.Parallel()

	req := ask{Query: "replayed"}
	req.Hold()
	req.Withdraw(1)
	req.Release()
}

// A Group made elsewhere, with its options, answers once wrapped. Publish does not wait for a subscriber added through
// the embedded Group.
func TestGroup_Wrap(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := &reply.Group[string, int]{Group: multicast.New(multicast.WithSerial[ask]())}
		t.Cleanup(func() { _ = g.Close() })
		id := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) { return len(query), nil })
		if _, err := g.Group.Subscribe(subscribeCtx(t), func(context.Context, uint64, ask) error { return nil }); err != nil {
			t.Fatalf("multicast.Group.Subscribe: %v", err)
		}

		start := time.Now()
		equal(t, publish(t, g, "north"), results{id: {Value: 5}})
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Publish returned after %v, want at once", elapsed)
		}
	})
}

// full is a holder with no room.
type full struct{}

func (full) Hold(message.Message[ask], time.Time) (int, bool) { return 0, false }

func (full) Release(time.Time, bool) (message.Message[ask], time.Duration, bool) {
	return message.Message[ask]{}, 0, false
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

func subscribe(
	t *testing.T, g *reply.Group[string, int], handle func(context.Context, uint64, string) (int, error),
	options ...subscriber.Option[ask],
) uint64 {
	t.Helper()
	id, err := g.Subscribe(subscribeCtx(t), handle, options...)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	return id
}

// busy subscribes a subscriber answering a query with its length, and keeps it busy until release is closed.
func busy(t *testing.T, g *reply.Group[string, int], release <-chan struct{}, options ...subscriber.Option[ask]) uint64 {
	t.Helper()
	id := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) {
		if query == "block" {
			<-release
		}

		return len(query), nil
	}, options...)
	// Through the embedded Group, which returns once the subscriber has taken it.
	if n := g.Group.Publish(t.Context(), ask{Query: "block"}); n != 1 {
		t.Fatalf("multicast.Group.Publish returned %d, want 1", n)
	}

	return id
}

// recovering returns a handle failing twice, then answering how many calls it took. Only its subscriber's goroutine
// calls it.
func recovering() func(context.Context, uint64, string) (int, error) {
	var calls int

	return func(context.Context, uint64, string) (int, error) {
		calls++
		if calls < 3 {
			return calls, errOffline
		}

		return calls, nil
	}
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
