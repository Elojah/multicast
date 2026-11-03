<p align="center">
  <img src="assets/logo.png" alt="multicast" width="180">
</p>

# multicast

[![CI](https://github.com/Elojah/multicast/actions/workflows/ci.yml/badge.svg)](https://github.com/Elojah/multicast/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/elojah/multicast.svg)](https://pkg.go.dev/github.com/elojah/multicast)

A generic in-process fan-out for Go: one `Publish` hands a message to every subscriber, and each subscriber handles
its messages one at a time, in its own goroutine. No dependencies, and it runs on TinyGo.

```sh
go get github.com/elojah/multicast
```

## Quick start

```go
g := multicast.New[string]()

id, err := g.Subscribe(ctx, func(ctx context.Context, id uint64, msg string) error {
 fmt.Println(id, "got", msg)
 return nil
}, subscriber.WithErrorHandler[string](func(ctx context.Context, err error) {
 log.Println(err)
}))
if err != nil {
 return err
}

n := g.Publish(ctx, "hello") // how many subscribers took it

err = g.Unsubscribe(ctx, id)
```

`SubscribeSeq` returns an iterator instead, whose loop body replaces `handle`. Breaking out unsubscribes:

```go
_, seq, err := g.SubscribeSeq(ctx)
...
for msg, fail := range seq {
 fail(save(msg)) // nil, or never calling fail, means handled
}
```

| Package | Holds |
| --- | --- |
| [`multicast`](https://pkg.go.dev/github.com/elojah/multicast) | `Group`, `WithSerial`, `WithDefaultSubscriberOptions`. |
| [`subscriber`](https://pkg.go.dev/github.com/elojah/multicast/subscriber) | Subscriber options, error types, `Stats`, `Holder`. |
| [`message`](https://pkg.go.dev/github.com/elojah/multicast/message) | `Publish` options, and `Batch` with `Slice` and `Map`. |
| [`middleware`](https://pkg.go.dev/github.com/elojah/multicast/middleware) | `Recover`, `History`, `MaxAge`, `WrapError`, `Retry`, and `FirstN` with `Race`. |
| [`store`](https://pkg.go.dev/github.com/elojah/multicast/store) | Queues for dead letters: `Ring` in memory, `File` on disk, `Drain`, `Enqueue`, `Filter`. |
| [`holder`](https://pkg.go.dev/github.com/elojah/multicast/holder) | Holders: `Reduce`, `NewLatest`, `Reorder`, `Jitter`, `Batch`. |
| [`filter`](https://pkg.go.dev/github.com/elojah/multicast/filter) | Stateful filters: `Changed`, `Every`, per key with `WithKey`, with `WithHeartbeat` and `WithHeartbeatCount`. |
| [`reply`](https://pkg.go.dev/github.com/elojah/multicast/reply) | `Group`, whose subscribers answer a query, `Request`, and `FirstN`. |
| [`reply/holder`](https://pkg.go.dev/github.com/elojah/multicast/reply/holder) | Holders for requests: `NewLatest`, answering those it merges away, `NewReorder` and `NewJitter`, by query. |

## Delivery

The mode is a message option, passed to `Publish` or set as a subscriber's default with
`subscriber.WithDefaultMessageOptions`.

| Mode | Option | `Publish` waits for |
| --- | --- | --- |
| Sync (default) | `message.WithSync` | Each subscriber in turn. |
| Buffered | `subscriber.WithBuffer(n)` | Nothing until the buffer is full, then as sync. |
| Parallel | `message.WithParallel` | Every subscriber at once. |
| Async | `message.WithAsync` | Nothing: each send runs in its own goroutine, out of order. |
| Non-blocking | `message.WithNonBlocking` | Nothing: a busy subscriber misses the message. |

Delivery is at most once, in publish order from one goroutine, except async. `Publish` gives up on a subscriber
once its ctx is done or the message's timeout runs out (`message.WithTimeout`, `subscriber.WithTimeout`), and the error
handlers learn why. `subscriber.WithAsyncLimit(n)` bounds the async sends waiting on a subscriber.

`multicast.New(multicast.WithSerial[T]())` makes each `Publish` wait for the one under way to return,
so that messages from every goroutine reach every subscriber in the same order, async sends aside.

`subscriber.WithFilter(keep)` skips what `keep` rejects before any send. `message.WithSubscriberFilter(keep)` hands one
message only to the subscribers `keep` picks by ID, and `message.WithVisit(visit)` calls `visit` with each of their IDs,
before their own filters. Package `filter` holds stateful filters, so give each subscriber its own:

```go
// Once the temperature has moved by half a degree since the last one passed, and at most once a minute.
id, err := g.Subscribe(ctx, display, subscriber.WithFilter(filter.Changed(func(prev, next float64) bool { return math.Abs(next-prev) >= 0.5 })))
id, err = g.Subscribe(ctx, uplink, subscriber.WithFilter(filter.Every[float64](time.Minute)))
// The same deadband for each sensor apart, and each one's reading at least once an hour, however steady.
id, err = sensors.Subscribe(ctx, trend, subscriber.WithFilter(filter.Changed(
	func(prev, next reading) bool { return math.Abs(next.Celsius-prev.Celsius) >= 0.5 },
	filter.WithKey(func(r reading) string { return r.Sensor }),
	filter.WithHeartbeat[reading](time.Hour),
)))
```

`multicast.WithDefaultSubscriberOptions(build)` gives each subscriber the options `build` returns, before its
`Subscribe`'s own, which override them. `build` runs for each subscriber, so that each gets its own filter:

```go
sensors := multicast.New(multicast.WithDefaultSubscriberOptions(func() []subscriber.Option[reading] {
	return []subscriber.Option[reading]{
		subscriber.WithBuffer[reading](16),
		subscriber.WithFilter(filter.Every[reading](time.Second, filter.WithKey(func(r reading) string { return r.Sensor }))),
	}
}))
```

## Errors

Errors go to `subscriber.WithErrorHandler`, then to `message.WithErrorHandler`. Without either, they are discarded.

| Error | When |
| --- | --- |
| `handle`'s error, as is | `handle`, its middlewares, or `fail` for `SubscribeSeq`, failed. |
| `*subscriber.HandleError` | The same, wrapped by `middleware.WrapError`. |
| `*subscriber.PanicError` | `middleware.Recover` recovered a panic. Without it, the program crashes. |
| `*subscriber.ExpiredError` | `middleware.MaxAge` found the message too old. |
| `*subscriber.TimeoutError` | `Publish` gave up waiting. |
| `*subscriber.DroppedError` | A non-blocking or async `Publish`, or a holder, had no room. |
| `*subscriber.ClosedError` | The subscriber was unsubscribed before handling the message. |
| `*subscriber.EvictedError` | Wraps the error that evicted the subscriber (`subscriber.WithEvict`). |
| `*subscriber.StoreError` | Wraps a loss the dead-letter store failed to keep. |

Each matches a sentinel with `errors.Is` (`subscriber.ErrTimeout`, …) without needing the message type.

## Middleware

`subscriber.WithMiddleware` wraps `handle`, the first middleware outermost, in the subscriber's goroutine:

```go
id, err := g.Subscribe(ctx, handle, subscriber.WithMiddleware(
 middleware.Recover[string](),                   // a panic becomes a *subscriber.PanicError
 middleware.History[string](history),            // each message handled is put in history
 middleware.MaxAge[string](time.Minute, sentAt), // an older message becomes a *subscriber.ExpiredError
 middleware.WrapError[string](),                 // an error becomes a *subscriber.HandleError
 middleware.Retry[string](middleware.RetryPolicy{Attempts: 3, Delay: 10 * time.Millisecond, Multiplier: 2}),
))
```

Keep that order: `Recover` first catches panics in every later middleware, and `Retry` after `WrapError` retries raw
errors. With `SubscribeSeq`, middlewares wrap the loop body, but `Recover` cannot catch a panic in it.

`middleware.FirstN(n, ...)`, right after `Recover`, makes subscribers race for each message carrying a
`middleware.Race`: the first n to handle it win, and the n-th cancels the ctx of the others still handling it, and makes
those yet to start skip it. A loser reports no error, but a subscriber that fails while the race is on does, so the
dead letters may keep a message another subscriber handles later. `race.Stop()` ends a race early, as if it had its
winners. `race.Done()` stays open if fewer than n win, so wait on a ctx too:

```go
type upload struct {
 data []byte
 race *middleware.Race
}

racing := subscriber.WithMiddleware(middleware.Recover[upload](), middleware.FirstN(1, func(u upload) *middleware.Race { return u.race }))
_, err := uplinks.Subscribe(ctx, cellular, racing)
_, err = uplinks.Subscribe(ctx, lora, racing)
...
race := middleware.NewRace()
defer race.Stop()
uplinks.Publish(ctx, upload{data: data, race: race})
select {
case <-race.Done(): // race.Winners()[0] sent it
case <-ctx.Done():
}
```

Keep `FirstN` before `Retry`, whose wait then ends with the race. One outside it waits out its delay, as a `Retry` from
`multicast.WithDefaultSubscriberOptions` does around a `FirstN` given to `Subscribe`: give both as defaults.

## Holding and batches

`subscriber.WithHolder` holds each message the subscriber takes until the holder releases it to `handle`, so that a
burst reaches `handle` as fewer messages, or in order. Package `holder` keeps the latest of a window (`NewLatest`),
reduces it (`NewReduce`), sorts it (`NewReorder`), sorts a jittery stream (`NewJitter`), or batches it (`NewBatch`):

```go
id, err := g.Subscribe(ctx, display, subscriber.WithHolder(holder.NewLatest[Reading](time.Second)))
```

`PublishBatch` hands a `message.Batch` to every subscriber in one send. A subscriber with a batch handler
(`subscriber.WithBatchHandler`) gets it whole, one without gets each value through `handle`:

```go
g.PublishBatch(ctx, &message.Slice[Reading]{north, south, east})

id, err := g.Subscribe(ctx, handle, subscriber.WithBatchHandler(func(ctx context.Context, _ uint64, batch message.Batch[Reading]) error {
 return uplink.Send(ctx, slices.Collect(batch.All()))
}))
```

A lost batch is reported once, with the batch in the error. Don't change a batch once passed.
`subscriber.WithBatchMiddleware` wraps the batch handler, whichever option sets it, as `WithDefaultSubscriberOptions`
may.

## Answers

A `reply.Group` asks its subscribers a query and gathers their answers. Its `Subscribe` takes a `handle` that returns
a value, and its `Publish` waits for an answer from each subscriber the query reached, or until ctx is done:

```go
g := reply.New[string, float64]()

id, err := g.Subscribe(ctx, func(ctx context.Context, _ uint64, unit string) (float64, error) {
	return sensor.Read(ctx, unit)
})
...
ctx, cancel := context.WithTimeout(ctx, time.Second)
defer cancel()
results := g.Publish(ctx, "celsius") // each one's Value or Err, by subscriber ID
```

It embeds a `multicast.Group` of `reply.Request`s, which are messages like any other, so its other methods, filters,
delivery modes and middlewares apply as they are. `&reply.Group[Q, R]{Group: g}` wraps a Group made elsewhere. An error
answers once reported, so `middleware.Retry` answers its last attempt and `middleware.Recover` a panic, even from
`multicast.WithDefaultSubscriberOptions`. A request the library loses answers why, such as a `*subscriber.DroppedError`,
and one still unanswered once ctx is done a `*subscriber.TimeoutError`. `NewLatest` from `reply/holder`,
`holder.NewLatest` for requests, answers the ones it merges away with the last one's answer, as a holder of your own
does with `req.Merge`, such as `holder.NewReduce`'s reduce. A `SubscribeSeq` loop or a batch handler answers with
`req.Answer`, and the batch handler's error answers each request of the batch.

`Publish` waits for a holder's window, which serial `Publish` calls from one goroutine never share, and for
`middleware.Retry`'s attempts: `message.WithContext(ctx)` stops them with the `Publish`.

`reply.FirstN` is `middleware.FirstN` for queries: a subscriber that loses leaves the `Publish`, which returns the
winners, and those that failed before the race ended. It still waits for each loser to stop, and `race.Stop()` ends a
`Retry` inside `FirstN` that would hold up the next `Publish`:

```go
racing := subscriber.WithMiddleware(reply.FirstN[query, float64](1, func(q query) *middleware.Race { return q.race }))
...
race := middleware.NewRace()
defer race.Stop()
results := g.Publish(ctx, query{unit: "celsius", race: race})
```

## Dead letters and replay

`subscriber.WithDeadLetters` stores every message the subscriber loses, with its error. `subscriber.WithReplay` hands a
subscriber an iterator's values before any message, such as its dead letters from before a restart. `store.File` is a
queue on disk that does both, with a checksum per record so that a torn write is dropped on reopen:

```go
lost, err := store.OpenFile(dir, encode, decode, store.WithSyncInterval(time.Second), store.WithMaxSize(64<<20))
...
id, err := g.Subscribe(ctx, page, subscriber.WithReplay(lost.Replay(ctx)), subscriber.WithDeadLetters[Alert](lost))
...
g.Shutdown(ctx)
lost.Close()
```

To store and forward, subscribe `store.Enqueue(queue)` as `handle`, and let `store.Drain` hand each message to a slow
sink, in order, so that `Publish` never waits for it.

## Lifecycle

- `Unsubscribe` and `Close` never wait, so `handle` can call them. An unsubscribed subscriber still handles what it
  took, unless `subscriber.WithUnsubscribeDiscard`, which reports it as `*subscriber.ClosedError` instead.
- A subscriber is unsubscribed once its `Subscribe` ctx is done, unless `subscriber.WithDetachedContext`. That ctx is
  what `handle` and the error handlers get, unless `message.WithContext` replaces it for one message. The `Publish`
  ctx only bounds the wait, but async sends keep using it after `Publish` returns.
- `subscriber.WithEvict(evict)` unsubscribes a subscriber on the first error `evict` picks, such as a timeout.
- `subscriber.WithDone(ctx, done)` sends the subscriber's ID once it is done with what it took.
- `Shutdown(ctx)` is `Close`, then waits until every subscriber is done, or ctx is. Call it on SIGTERM.
- `Stats` returns each subscriber's counters: queued, handled, failed, lost, held, and how long `handle` has been
  running, for a watchdog.

A subscriber reconnects in place: `handle` or a middleware dials again, `middleware.Retry` retries, and the buffer
keeps what comes meanwhile in order ([`10-reconnect`](examples/10-reconnect/main.go)).

## Examples

[`examples/`](examples) holds runnable programs: `go run ./examples/01-basic`.

| Example | Shows |
| --- | --- |
| [`01-basic`](examples/01-basic/main.go) | `Subscribe`, `Publish`, and `handle` unsubscribing itself. |
| [`02-delivery`](examples/02-delivery/main.go) | Buffered, non-blocking, timed out and async delivery. |
| [`03-errors`](examples/03-errors/main.go) | Error handler, `middleware.Recover` and `middleware.WrapError`. |
| [`04-iterator`](examples/04-iterator/main.go) | `SubscribeSeq` and `fail`. |
| [`05-middleware`](examples/05-middleware/main.go) | `middleware.Retry` with backoff, and a middleware of your own. |
| [`06-filter`](examples/06-filter/main.go) | `subscriber.WithFilter` with `filter.Changed` and `filter.Every`. |
| [`07-holder`](examples/07-holder/main.go) | `holder.NewLatest` and `holder.Reduce` for bursts of readings. |
| [`08-batch`](examples/08-batch/main.go) | `PublishBatch`, `subscriber.WithBatchHandler` and `holder.NewBatch`. |
| [`09-file`](examples/09-file/main.go) | Dead letters on disk with `store.File`, replayed after a restart. |
| [`10-reconnect`](examples/10-reconnect/main.go) | A subscriber reconnecting in place, its buffer keeping order. |
| [`11-mqtt`](examples/11-mqtt/main.go) | MQTT fanned out to a rule, a local store, and an uplink that stores and forwards. |
| [`12-modbus`](examples/12-modbus/main.go) | A PLC polled over Modbus TCP, and a rule that retries and reconnects. |
| [`13-reply`](examples/13-reply/main.go) | A `reply.Group` asking every sensor for its reading. |
| [`14-reply-batch`](examples/14-reply-batch/main.go) | A bus answering the queries that come together in one request, and its error answering them all. |

`11-mqtt` and `12-modbus` are modules of their own, to keep their dependencies out of the library's go.mod.
`go run -C examples/11-mqtt .` needs an MQTT broker at `MQTT_ADDR` (`localhost:1883` by default). `12-modbus` simulates
its PLC in process.

## Performance

Time per message per subscriber, the median of 6 runs of `make bench` at `e28de1e` (Go 1.26.1, Intel Core Ultra 5
226V, 8 cores). Subscribers are buffered and waited for once, after the last `Publish`, unless "until handled".

| Benchmark | 1 subscriber | 10 | 100 | 1000 |
| --- | --- | --- | --- | --- |
| Sync | 223 ns | 152 ns | 220 ns | 341 ns |
| Sync, until handled | 652 ns | 697 ns | 685 ns | 482 ns |
| Sync, from 8 goroutines | 431 ns | 126 ns | 68 ns | 59 ns |
| Parallel | 1.44 µs | 1.46 µs | 858 ns | 722 ns |
| Async | 2.13 µs | 445 ns | 431 ns | 471 ns |
| Batch of 16, value by value | 94 ns | 34 ns | 19 ns | 19 ns |
| Batch of 16, to a batch handler | 15 ns | 10 ns | 15 ns | 22 ns |

A `Publish` without options allocates nothing in sync, buffered and non-blocking modes. Async allocates once per
subscriber (130 B), parallel twice (270 B). A `Subscribe` and `Unsubscribe` pair takes 1.2 µs and 12 allocations
(1.1 KiB).

A `reply.Group`'s `Publish` until every subscriber has answered, next to a `multicast.Group`'s until every subscriber
has handled the message, both the median of the same 6 runs: time per answer.

| Benchmark | 1 subscriber | 10 | 100 | 1000 |
| --- | --- | --- | --- | --- |
| `multicast`, sync, until handled | 691 ns | 657 ns | 653 ns | 475 ns |
| `multicast`, parallel, until handled | 2.00 µs | 1.81 µs | 879 ns | 763 ns |
| `reply`, sync | 2.09 µs | 1.48 µs | 1.09 µs | 1.13 µs |
| `reply`, parallel | 3.83 µs | 2.32 µs | 1.25 µs | 1.43 µs |
| `reply`, sync, every subscriber failing | 2.20 µs | 1.49 µs | 1.04 µs | 1.13 µs |

A `reply` `Publish` allocates 13 times (960 B), then 3.3 to 4.9 times per subscriber (190 to 270 B), and once more for
each error (64 B).

## Development

```sh
make check     # golangci-lint, and go test -race -shuffle=on -cpu 1,4
make stress    # the stress test, 50 times
make benchcmp  # benchstat of main against the working tree
make tinygo    # with TinyGo: run each example and build it for a Raspberry Pi Pico
```

The library needs TinyGo 0.43 or later: 0.42 can deadlock a `Close` racing `Subscribe`s
([tinygo#5692](https://github.com/tinygo-org/tinygo/issues/5692)). Under TinyGo, `PanicError.Stack` is nil, and the
tests need Go, since they use `testing/synctest`.

## License

[MIT](LICENSE).
