package reply_test

import (
	"cmp"
	"context"
	"errors"
	"iter"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
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

var errStore = errors.New("store full")

// batch is what a batch handler gets.
type batch = message.Batch[ask]

// handleFunc is what Group.Subscribe takes.
type handleFunc = func(ctx context.Context, id uint64, query string) (int, error)

// retryPolicy retries twice, a second apart.
var retryPolicy = middleware.RetryPolicy{Attempts: 3, Delay: time.Second}

// Every middleware of package middleware, FirstN through reply.FirstN, answers each request under a Group, passed to
// Subscribe or from multicast.WithDefaultSubscriberOptions, and only Retry adds to Publish's wait: its delays.
func TestMiddlewares(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		middleware func() subscriber.Middleware[ask]
		handle     func() handleFunc
		want       reply.Result[int]
		elapsed    time.Duration
	}{
		{name: "Recover", middleware: middleware.Recover[ask], handle: always(answering), want: reply.Result[int]{Value: 5}},
		{
			name: "Recover/panic", middleware: middleware.Recover[ask], handle: always(panicking),
			want: reply.Result[int]{Err: subscriber.ErrPanic},
		},
		{name: "WrapError", middleware: middleware.WrapError[ask], handle: always(failing), want: reply.Result[int]{Err: errOffline}},
		{
			name: "Retry", middleware: func() subscriber.Middleware[ask] { return middleware.Retry[ask](retryPolicy) },
			handle: recovering, want: reply.Result[int]{Value: 3}, elapsed: 2 * time.Second,
		},
		{
			name: "Retry/fail", middleware: func() subscriber.Middleware[ask] { return middleware.Retry[ask](retryPolicy) },
			handle: always(failing), want: reply.Result[int]{Err: errOffline}, elapsed: 2 * time.Second,
		},
		{
			name: "MaxAge", middleware: func() subscriber.Middleware[ask] { return middleware.MaxAge(time.Minute, fresh) },
			handle: always(answering), want: reply.Result[int]{Value: 5},
		},
		{
			name: "MaxAge/expired", middleware: func() subscriber.Middleware[ask] { return middleware.MaxAge(time.Minute, stale) },
			handle: always(answering), want: reply.Result[int]{Err: subscriber.ErrExpired},
		},
		{
			name: "History", middleware: func() subscriber.Middleware[ask] { return middleware.History[ask](store.NewRing[ask](1)) },
			handle: always(answering), want: reply.Result[int]{Value: 5},
		},
		{
			// Fewer subscribers than n: Publish returns all the same.
			name: "FirstN", middleware: func() subscriber.Middleware[ask] { return racing(2, raceOf(middleware.NewRace())) },
			handle: always(answering), want: reply.Result[int]{Value: 5},
		},
		{
			name: "FirstN/fail", middleware: func() subscriber.Middleware[ask] { return racing(2, raceOf(middleware.NewRace())) },
			handle: always(failing), want: reply.Result[int]{Err: errOffline},
		},
	} {
		for _, byDefault := range []bool{false, true} {
			t.Run(tt.name+"/"+placement(byDefault), func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					g, options := groupWith(t, byDefault, func() []subscriber.Option[ask] {
						return []subscriber.Option[ask]{subscriber.WithMiddleware(tt.middleware())}
					})
					id := subscribe(t, g, tt.handle(), options...)

					start := time.Now()
					equal(t, publish(t, g, "north"), results{id: tt.want})
					if elapsed := time.Since(start); elapsed != tt.elapsed {
						t.Errorf("Publish returned after %v, want %v", elapsed, tt.elapsed)
					}
				})
			})
		}
	}
}

// WrapError is redundant under a Group, whose Subscribe wraps handle's error already: passed to Subscribe, it adds
// nothing, and from WithDefaultSubscriberOptions, which run outside the answer, it adds a second *HandleError.
func TestMiddlewareWrapError(t *testing.T) {
	t.Parallel()

	for _, byDefault := range []bool{false, true} {
		t.Run(placement(byDefault), func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g, options := groupWith(t, byDefault, func() []subscriber.Option[ask] {
					return []subscriber.Option[ask]{subscriber.WithMiddleware(middleware.WrapError[ask]())}
				})
				id := subscribe(t, g, failing, options...)

				err := publish(t, g, "north")[id].Err
				var n int
				for handleErr := (*subscriber.HandleError[ask])(nil); errors.As(err, &handleErr); err = handleErr.Err {
					n++
				}
				if want := map[bool]int{false: 1, true: 2}[byDefault]; n != want {
					t.Errorf("answer wraps %d *HandleError, want %d", n, want)
				}
			})
		})
	}
}

// History puts every request handled, and its failed Put answers its error although handle succeeded, unless it runs
// outside the answer, from WithDefaultSubscriberOptions.
func TestMiddlewareHistory(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		byDefault bool
		put       error
		want      reply.Result[int]
	}{
		{name: "subscribe", want: reply.Result[int]{Value: 5}},
		{name: "default", byDefault: true, want: reply.Result[int]{Value: 5}},
		{name: "subscribe/put fails", put: errStore, want: reply.Result[int]{Err: errStore}},
		{name: "default/put fails", byDefault: true, put: errStore, want: reply.Result[int]{Value: 5}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				records := make(chan subscriber.Record[ask], 1)
				history := store.PutFunc[ask](func(_ context.Context, r subscriber.Record[ask]) error {
					records <- r

					return tt.put
				})
				g, options := groupWith(t, tt.byDefault, func() []subscriber.Option[ask] {
					return []subscriber.Option[ask]{subscriber.WithMiddleware(middleware.History[ask](history))}
				})
				id := subscribe(t, g, answering, options...)

				equal(t, publish(t, g, "north"), results{id: tt.want})
				// Outside the answer, Put comes after it.
				synctest.Wait()
				select {
				case r := <-records:
					if r.SubscriberID != id || r.Message.Query != "north" || r.Err != nil {
						t.Errorf("History put %+v, want subscriber %d's request north, with no error", r, id)
					}
				default:
					t.Error("History put nothing")
				}
			})
		})
	}
}

// A request History or dead letters keep holds none of its Publish's answers once Publish has returned, a late one
// included.
func TestMiddlewareHistory_Retention(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		kept := make(chan ask, 2)
		history := subscriber.WithMiddleware(middleware.History[ask](store.PutFunc[ask](
			func(_ context.Context, r subscriber.Record[ask]) error {
				kept <- r.Message

				return nil
			})))
		quick := subscribe(t, g, answering, history)
		release := make(chan struct{})
		slow := subscribe(t, g, func(context.Context, uint64, string) (int, error) {
			<-release

			return 2, nil
		}, history)

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		equal(t, g.Publish(ctx, "north"), results{quick: {Value: 5}, slow: {Err: subscriber.ErrTimeout}})
		close(release)
		for range 2 {
			if n := reply.Retained(<-kept); n != 0 {
				t.Errorf("request kept by History retains %d results, want 0", n)
			}
		}
	})
}

// Retry waits in the subscriber's goroutine, for handle's ctx: Publish's ctx stops it only once passed with
// message.WithContext. Otherwise the subscriber goes on retrying after Publish has returned, and holds up the next one.
func TestMiddlewareRetry_Context(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		// withContext passes a ctx done after a second with message.WithContext, else Publish's ctx is.
		withContext bool
		want        reply.Result[int]
		// next is how long the next Publish waits.
		next time.Duration
	}{
		{name: "publish ctx", want: reply.Result[int]{Err: subscriber.ErrTimeout}, next: 2*time.Hour - time.Second},
		{name: "message ctx", withContext: true, want: reply.Result[int]{Err: errOffline}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup(t)
				id := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) {
					if query == "north" {
						return 0, errOffline
					}

					return len(query), nil
				}, subscriber.WithMiddleware(middleware.Retry[ask](middleware.RetryPolicy{Attempts: 3, Delay: time.Hour})))

				// Two ctx, rather than one passed to both, so that Publish never races handle's answer.
				timeout, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				ctx, options := timeout, []message.Option[ask](nil)
				if tt.withContext {
					ctx, options = t.Context(), []message.Option[ask]{message.WithContext[ask](timeout)}
				}
				start := time.Now()
				equal(t, g.Publish(ctx, "north", options...), results{id: tt.want})
				if elapsed := time.Since(start); elapsed != time.Second {
					t.Errorf("Publish returned after %v, want %v", elapsed, time.Second)
				}

				ctx, cancel = context.WithTimeout(t.Context(), 3*time.Hour)
				defer cancel()
				start = time.Now()
				equal(t, g.Publish(ctx, "south"), results{id: {Value: 5}})
				if elapsed := time.Since(start); elapsed != tt.next {
					t.Errorf("next Publish returned after %v, want %v", elapsed, tt.next)
				}
			})
		})
	}
}

// A SubscribeSeq loop under Retry answers once its last attempt does, and one under MaxAge never sees an expired
// request, which answers its *ExpiredError.
func TestSubscribeSeq_Middlewares(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		middleware subscriber.Middleware[ask]
		want       reply.Result[int]
		elapsed    time.Duration
	}{
		{name: "Retry", middleware: middleware.Retry[ask](retryPolicy), want: reply.Result[int]{Value: 3}, elapsed: 2 * time.Second},
		{name: "MaxAge", middleware: middleware.MaxAge(time.Minute, stale), want: reply.Result[int]{Err: subscriber.ErrExpired}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup(t)
				id, seq, err := g.SubscribeSeq(subscribeCtx(t), subscriber.WithMiddleware(tt.middleware))
				if err != nil {
					t.Fatalf("SubscribeSeq: %v", err)
				}
				go func() {
					var attempts int
					for req, fail := range seq {
						if attempts++; attempts < 3 {
							fail(errOffline)

							continue
						}
						req.Answer(id, attempts, nil)
					}
				}()

				start := time.Now()
				equal(t, publish(t, g, "north"), results{id: tt.want})
				if elapsed := time.Since(start); elapsed != tt.elapsed {
					t.Errorf("Publish returned after %v, want %v", elapsed, tt.elapsed)
				}
			})
		})
	}
}

// A batch handler's error, or its middlewares', answers every request of the batch at once, as a *HandleError about the
// batch unless the library made it, with Subscribe or SubscribeSeq.
func TestBatchHandler_Middlewares(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		handle      func() subscriber.Handler[batch]
		middlewares []subscriber.Middleware[batch]
		want        reply.Result[int]
		elapsed     time.Duration
	}{
		{name: "error", handle: always[subscriber.Handler[batch]](failingBatch), want: reply.Result[int]{Err: errOffline}},
		{
			name: "Recover", handle: always[subscriber.Handler[batch]](panickingBatch), middlewares: []subscriber.Middleware[batch]{middleware.Recover[batch]()},
			want: reply.Result[int]{Err: subscriber.ErrPanic},
		},
		{
			name: "WrapError", handle: always[subscriber.Handler[batch]](failingBatch), middlewares: []subscriber.Middleware[batch]{middleware.WrapError[batch]()},
			want: reply.Result[int]{Err: errOffline},
		},
		{
			name: "Retry", handle: recoveringBatch, middlewares: []subscriber.Middleware[batch]{middleware.Retry[batch](retryPolicy)},
			want: reply.Result[int]{Value: 3}, elapsed: 2 * time.Second,
		},
		{
			name: "MaxAge", handle: always[subscriber.Handler[batch]](failingBatch), middlewares: []subscriber.Middleware[batch]{
				middleware.MaxAge(time.Minute, func(batch) time.Time { return time.Now().Add(-time.Hour) }),
			},
			want: reply.Result[int]{Err: subscriber.ErrExpired},
		},
	} {
		for _, seq := range []bool{false, true} {
			t.Run(tt.name+map[bool]string{false: "/Subscribe", true: "/SubscribeSeq"}[seq], func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					g := newGroup(t)
					options := []subscriber.Option[ask]{
						subscriber.WithHolder(holder.NewBatch[ask](time.Second, 2)),
						subscriber.WithBatchHandler(tt.handle(), tt.middlewares...),
					}
					var id uint64
					if seq {
						var requests iter.Seq2[ask, func(error)]
						var err error
						if id, requests, err = g.SubscribeSeq(subscribeCtx(t), options...); err != nil {
							t.Fatalf("SubscribeSeq: %v", err)
						}
						go func() {
							for range requests {
								t.Error("the loop got a request of a batch")
							}
						}()
					} else {
						id = subscribe(t, g, unbatched(t), options...)
					}

					got, elapsed := publishAll(t, g, "north", "south")
					for i := range got {
						equal(t, got[i], results{id: tt.want})
						if elapsed[i] != tt.elapsed {
							t.Errorf("Publish %d returned after %v, want %v", i, elapsed[i], tt.elapsed)
						}
					}
				})
			})
		}
	}
}

// A batch handler's error is a *HandleError about its batch, unless the library made it, such as Recover's.
func TestBatchHandler_Attribute(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		handle      subscriber.Handler[batch]
		middlewares []subscriber.Middleware[batch]
		check       func(err error) bool
	}{
		{name: "error", handle: failingBatch, check: func(err error) bool {
			var handleErr *subscriber.HandleError[batch]

			return errors.As(err, &handleErr) && handleErr.Message.Len() == 2 && errors.Is(handleErr.Err, errOffline)
		}},
		{name: "Recover", handle: panickingBatch, middlewares: []subscriber.Middleware[batch]{middleware.Recover[batch]()}, check: func(err error) bool {
			var panicErr *subscriber.PanicError[batch]
			var handleErr *subscriber.HandleError[batch]

			return errors.As(err, &panicErr) && panicErr.Message.Len() == 2 && !errors.As(err, &handleErr)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup(t)
				id := subscribe(t, g, unbatched(t), subscriber.WithHolder(holder.NewBatch[ask](time.Second, 2)),
					subscriber.WithBatchHandler(tt.handle, tt.middlewares...))

				got, _ := publishAll(t, g, "north", "south")
				for i := range got {
					if err := got[i][id].Err; !tt.check(err) {
						t.Errorf("Publish %d answered %v", i, err)
					}
				}
			})
		})
	}
}

// Each holder of package holder answers every request it releases, once its window ends: d after the first request
// for Batch, Reduce and Reorder, or at once when a Batch is full, and d after each one for Jitter. Of the requests a
// holder merges away, only those merged with Request.Merge are answered: holder.NewLatest's wait for ctx.
func TestHolders(t *testing.T) {
	t.Parallel()

	byQuery := func(a, b ask) int { return cmp.Compare(a.Query, b.Query) }
	for _, tt := range []struct {
		name    string
		holder  func() subscriber.Holder[ask]
		queries []string
		want    []reply.Result[int]
		elapsed []time.Duration
		handled []string
	}{
		{
			name:    "Batch",
			holder:  func() subscriber.Holder[ask] { return holder.NewBatch[ask](time.Second, 0) },
			queries: []string{"north", "south-west"},
			want:    []reply.Result[int]{{Value: 5}, {Value: 10}},
			elapsed: []time.Duration{time.Second, time.Second},
			handled: []string{"north", "south-west"},
		},
		{
			name:    "Batch/full",
			holder:  func() subscriber.Holder[ask] { return holder.NewBatch[ask](time.Second, 2) },
			queries: []string{"north", "south-west"},
			want:    []reply.Result[int]{{Value: 5}, {Value: 10}},
			elapsed: []time.Duration{0, 0},
			handled: []string{"north", "south-west"},
		},
		{
			name:    "Reorder",
			holder:  func() subscriber.Holder[ask] { return holder.NewReorderFunc(time.Second, byQuery) },
			queries: []string{"south-west", "north"},
			want:    []reply.Result[int]{{Value: 10}, {Value: 5}},
			elapsed: []time.Duration{time.Second, time.Second},
			handled: []string{"north", "south-west"},
		},
		{
			name:    "Jitter",
			holder:  func() subscriber.Holder[ask] { return holder.NewJitterFunc(time.Second, byQuery) },
			queries: []string{"south-west", "north"},
			want:    []reply.Result[int]{{Value: 10}, {Value: 5}},
			elapsed: []time.Duration{time.Second, time.Second},
			handled: []string{"north", "south-west"},
		},
		{
			name: "Reduce",
			holder: func() subscriber.Holder[ask] {
				// The latest request per query, answering for those it replaces.
				return holder.NewReduce(time.Second, func(held []ask, next ask) []ask {
					for i, older := range held {
						if older.Query == next.Query {
							held[i] = next.Merge(older)

							return held
						}
					}

					return append(held, next)
				})
			},
			queries: []string{"north", "south-west", "north"},
			want:    []reply.Result[int]{{Value: 5}, {Value: 10}, {Value: 5}},
			elapsed: []time.Duration{time.Second, time.Second, time.Second},
			handled: []string{"north", "south-west"},
		},
		{
			name:    "NewLatest",
			holder:  func() subscriber.Holder[ask] { return holder.NewLatest[ask](time.Second) },
			queries: []string{"north", "south-west"},
			want:    []reply.Result[int]{{Err: subscriber.ErrTimeout}, {Value: 10}},
			elapsed: []time.Duration{deadlockTimeout, time.Second},
			handled: []string{"south-west"},
		},
		{
			name:    "reply/NewLatest",
			holder:  func() subscriber.Holder[ask] { return replyholder.NewLatest[string, int](time.Second) },
			queries: []string{"north", "south-west"},
			want:    []reply.Result[int]{{Value: 10}, {Value: 10}},
			elapsed: []time.Duration{time.Second, time.Second},
			handled: []string{"south-west"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup(t)
				var handled []string // Only the subscriber's goroutine appends, before answering.
				id := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) {
					handled = append(handled, query)

					return len(query), nil
				}, subscriber.WithHolder(tt.holder()))

				got, elapsed := publishAll(t, g, tt.queries...)
				for i := range got {
					equal(t, got[i], results{id: tt.want[i]})
					if elapsed[i] != tt.elapsed[i] {
						t.Errorf("Publish of %s returned after %v, want %v", tt.queries[i], elapsed[i], tt.elapsed[i])
					}
				}
				if !slices.Equal(handled, tt.handled) {
					t.Errorf("handle got %v, want %v", handled, tt.handled)
				}
				addUp(t, g)
			})
		})
	}
}

// A Publish waits for its answer, so serial ones from one goroutine never share a window: each waits all of it.
func TestHolderBatch_Serial(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		id := subscribe(t, g, answering, subscriber.WithHolder(holder.NewBatch[ask](time.Second, 2)))

		for _, query := range []string{"north", "south"} {
			start := time.Now()
			equal(t, publish(t, g, query), results{id: {Value: 5}})
			if elapsed := time.Since(start); elapsed != time.Second {
				t.Errorf("Publish of %s returned after %v, want %v", query, elapsed, time.Second)
			}
		}
	})
}

// Publish returns the winners and those that failed before the race ended, and no loser, whatever its handle returned
// and wherever FirstN sits. It returns once the last winner has answered and the losers have stopped. With a Retry
// inside FirstN, the one failing is still waiting when the race ends, so it loses too.
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
					slow := func(d time.Duration, value int) handleFunc {
						return func(context.Context, uint64, string) (int, error) {
							time.Sleep(d)

							return value, nil
						}
					}
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

// A loser ignoring its ctx and answering a value once the race is over holds Publish up, but stays out of it, even from
// the default options, where it answers before FirstN knows it lost.
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
				slow := func(d time.Duration, value int) handleFunc {
					return func(context.Context, uint64, string) (int, error) {
						time.Sleep(d)

						return value, nil
					}
				}
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
				winner := subscribe(t, g, func(context.Context, uint64, string) (int, error) {
					time.Sleep(time.Second)

					return 5, nil
				}, option)

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

		got, elapsed := publishAll(t, g, "north", "south")
		for i := range got {
			equal(t, got[i], results{})
			if elapsed[i] != time.Second {
				t.Errorf("Publish %d returned after %v, want %v", i, elapsed[i], time.Second)
			}
		}
	})
}

// A Retry inside FirstN outlives a Publish whose ctx ends first, as in TestMiddlewareRetry_Context, but Race.Stop ends
// it, so that it holds up no later Publish.
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

// publishAll publishes each query from a goroutine of its own, the next once the previous one waits, and returns what
// each Publish returned and after how long. They all start at the same instant, since synctest.Wait takes no time.
func publishAll(t *testing.T, g *reply.Group[string, int], queries ...string) ([]results, []time.Duration) {
	t.Helper()
	got := make([]results, len(queries))
	elapsed := make([]time.Duration, len(queries))
	start := time.Now()
	var wg sync.WaitGroup
	for i, query := range queries {
		wg.Go(func() {
			got[i] = publish(t, g, query)
			elapsed[i] = time.Since(start)
		})
		// Once the subscriber holds the request, or has answered it.
		synctest.Wait()
	}
	wg.Wait()

	return got, elapsed
}

// addUp fails t unless each request every subscriber took is counted once, handled, failed or merged, none still held.
func addUp(t *testing.T, g *reply.Group[string, int]) {
	t.Helper()
	synctest.Wait()
	for _, s := range g.Stats() {
		if s.Held != 0 || s.Delivered != s.Handled+s.Failed+s.Merged {
			t.Errorf("subscriber %d: %+v, want Delivered = Handled + Failed + Merged, and nothing held", s.SubscriberID, s)
		}
	}
}

func always[F any](f F) func() F {
	return func() F { return f }
}

func answering(_ context.Context, _ uint64, query string) (int, error) { return len(query), nil }

func failing(context.Context, uint64, string) (int, error) { return 1, errOffline }

func panicking(context.Context, uint64, string) (int, error) { panic(errOffline) }

// racing makes a subscriber race for each request race returns a Race for.
func racing(n int, race func(query string) *middleware.Race) subscriber.Middleware[ask] {
	return reply.FirstN[string, int](n, race)
}

// raceOf returns what gives every request r.
func raceOf(r *middleware.Race) func(string) *middleware.Race {
	return func(string) *middleware.Race { return r }
}

func fresh(ask) time.Time { return time.Now() }

func stale(ask) time.Time { return time.Now().Add(-time.Hour) }

func failingBatch(context.Context, uint64, batch) error { return errOffline }

func panickingBatch(context.Context, uint64, batch) error { panic(errOffline) }

// recoveringBatch returns a batch handler failing twice, then answering each request how many calls it took. Only its
// subscriber's goroutine calls it.
func recoveringBatch() subscriber.Handler[batch] {
	var calls int

	return func(_ context.Context, id uint64, b batch) error {
		if calls++; calls < 3 {
			return errOffline
		}
		for req := range b.All() {
			req.Answer(id, calls, nil)
		}

		return nil
	}
}

// unbatched is a handle for a subscriber whose requests all go to its batch handler.
func unbatched(t *testing.T) handleFunc {
	t.Helper()

	return func(context.Context, uint64, string) (int, error) {
		t.Error("handle got a request of a batch")

		return 0, nil
	}
}
