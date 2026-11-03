package reply_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/elojah/multicast"
	"github.com/elojah/multicast/holder"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/middleware"
	"github.com/elojah/multicast/reply"
	replyholder "github.com/elojah/multicast/reply/holder"
	"github.com/elojah/multicast/store"
	"github.com/elojah/multicast/subscriber"
)

// benchSubscribers are the subscriber counts the benchmarks run with, as in package multicast's.
var benchSubscribers = []int{1, 10, 100, 1000}

var errBench = errors.New("bench")

// BenchmarkPublish measures one Publish until every subscriber has answered, per delivery mode, and with every
// subscriber failing, whose errors answer once reported. Package multicast's BenchmarkPublish measures the same Publish
// until every subscriber has handled the message, so the difference is what answering costs.
func BenchmarkPublish(b *testing.B) {
	modes := []struct {
		name    string
		err     error
		publish []message.Option[reply.Request[int, int]]
	}{
		{name: "sync"},
		{name: "parallel", publish: []message.Option[reply.Request[int, int]]{message.WithParallel[reply.Request[int, int]]()}},
		{name: "errors", err: errBench},
	}

	for _, subscribers := range benchSubscribers {
		for _, mode := range modes {
			b.Run(fmt.Sprintf("subscribers=%d/%s", subscribers, mode.name), func(b *testing.B) {
				g := reply.New[int, int]()
				b.Cleanup(func() { _ = g.Close() })
				for range subscribers {
					if _, err := g.Subscribe(b.Context(), func(_ context.Context, _ uint64, query int) (int, error) {
						return query, mode.err
					}); err != nil {
						b.Fatalf("Subscribe: %v", err)
					}
				}

				b.ReportAllocs()
				for b.Loop() {
					if results := g.Publish(b.Context(), 0, mode.publish...); len(results) != subscribers {
						b.Fatalf("Publish returned %d answers, want %d", len(results), subscribers)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*subscribers), "ns/answer")
			})
		}
	}
}

// benchCase is a subscriber's options, built for each one.
type benchCase struct {
	name    string
	options func() []subscriber.Option[ask]
}

// BenchmarkPublish_Middlewares measures each middleware of package middleware on its success path, as
// BenchmarkPublish_Options does.
func BenchmarkPublish_Middlewares(b *testing.B) {
	sent := time.Now()
	middlewares := func(m func() subscriber.Middleware[ask]) func() []subscriber.Option[ask] {
		return func() []subscriber.Option[ask] { return []subscriber.Option[ask]{subscriber.WithMiddleware(m())} }
	}
	benchOptions(b, []benchCase{
		{name: "none", options: func() []subscriber.Option[ask] { return nil }},
		{name: "Recover", options: middlewares(middleware.Recover[ask])},
		{name: "WrapError", options: middlewares(middleware.WrapError[ask])},
		{name: "Retry", options: middlewares(func() subscriber.Middleware[ask] {
			return middleware.Retry[ask](middleware.RetryPolicy{Attempts: 3})
		})},
		{name: "MaxAge", options: middlewares(func() subscriber.Middleware[ask] {
			return middleware.MaxAge(time.Hour, func(ask) time.Time { return sent })
		})},
		{name: "History", options: middlewares(func() subscriber.Middleware[ask] {
			return middleware.History[ask](store.NewRing[ask](1024))
		})},
	})
}

// BenchmarkPublish_Holders measures each holder with no window, so what the holder costs without its wait, as
// BenchmarkPublish_Options does.
func BenchmarkPublish_Holders(b *testing.B) {
	holders := func(h func() subscriber.Holder[ask]) func() []subscriber.Option[ask] {
		return func() []subscriber.Option[ask] { return []subscriber.Option[ask]{subscriber.WithHolder(h())} }
	}
	benchOptions(b, []benchCase{
		{name: "none", options: func() []subscriber.Option[ask] { return nil }},
		{name: "Batch", options: holders(func() subscriber.Holder[ask] { return holder.NewBatch[ask](0, 0) })},
		{name: "NewLatest", options: holders(func() subscriber.Holder[ask] { return holder.NewLatest[ask](0) })},
		{name: "Reorder", options: holders(func() subscriber.Holder[ask] { return replyholder.NewReorder[string, int](0) })},
		{name: "Jitter", options: holders(func() subscriber.Holder[ask] { return replyholder.NewJitter[string, int](0) })},
		{name: "reply/NewLatest", options: holders(func() subscriber.Holder[ask] { return replyholder.NewLatest[string, int](0) })},
	})
}

// benchOptions measures one sync Publish per case: through package multicast's Group until every subscriber has
// handled the request, then through a reply.Group until every subscriber has answered it. What a case costs is its
// time minus none's, under one Group and the other.
func benchOptions(b *testing.B, cases []benchCase) {
	b.Helper()
	for _, subscribers := range []int{1, 100} {
		for _, c := range cases {
			b.Run(fmt.Sprintf("subscribers=%d/%s/multicast", subscribers, c.name), func(b *testing.B) {
				g := multicast.New[ask]()
				b.Cleanup(func() { _ = g.Close() })
				var handled sync.WaitGroup
				for range subscribers {
					if _, err := g.Subscribe(b.Context(), func(context.Context, uint64, ask) error {
						handled.Done()

						return nil
					}, c.options()...); err != nil {
						b.Fatalf("Subscribe: %v", err)
					}
				}

				b.ReportAllocs()
				for b.Loop() {
					handled.Add(subscribers)
					g.Publish(b.Context(), ask{Query: "north"})
					handled.Wait()
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*subscribers), "ns/answer")
			})
			b.Run(fmt.Sprintf("subscribers=%d/%s/reply", subscribers, c.name), func(b *testing.B) {
				g := reply.New[string, int]()
				b.Cleanup(func() { _ = g.Close() })
				for range subscribers {
					if _, err := g.Subscribe(b.Context(), answering, c.options()...); err != nil {
						b.Fatalf("Subscribe: %v", err)
					}
				}

				b.ReportAllocs()
				for b.Loop() {
					if results := g.Publish(b.Context(), "north"); len(results) != subscribers {
						b.Fatalf("Publish returned %d answers, want %d", len(results), subscribers)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*subscribers), "ns/answer")
			})
		}
	}
}
