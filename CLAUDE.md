# CLAUDE.md

`github.com/elojah/multicast`, a generic in-process fan-out. Go 1.26.1, no dependency. Behaviour is in godoc and
the README, plans in TODO.md.

## Layout

- Imports go `multicast` → `subscriber` → `message`. `middleware` and `store` import `subscriber`, never
  `multicast`. `holder` imports only `message`, `filter` nothing from the library. `reply` imports `multicast`,
  `reply/holder` and `reply/middleware` import `reply` and their parent, and nothing in the library imports them.
- In `subscriber`, `deliver.go` runs in the `Publish`'s goroutine, `consume.go` and `holder.go` in the
  subscriber's, `report.go` in either. `inbox` owns the channel and its references, `counters` every counter.
- `examples/11-mqtt` and `12-modbus` are modules of their own (`replace ../..`), to keep their dependencies out of
  go.mod. `go test ./...` skips them, `make` covers them.

## Commands

```sh
go test -race ./...  # always -race
make check           # golangci-lint, tests shuffled with -cpu 1,4, each benchmark once
make stress          # TestStress 50 times
make benchcmp        # benchstat of main against the working tree
make tinygo          # TinyGo: pkg/gate's tests, each example run and checked, built for a Pico
```

Lint is `default: all` minus a list, tests included: mind `nlreturn`, `paralleltest` (`t.Parallel()` first), `err113`,
`godot` and `funcorder` (exported methods first).

## Conventions

- Comments are short: invariants, non-obvious whys, the public contract. State each fact once, where it belongs.
- Options have no prefix: `subscriber.WithBuffer`, `message.WithAsync`.
- `Subscriber` exports only what `Group` calls, so its invariants hold whatever a caller does.
- Errors are typed structs with a `SubscriberID` and an `Is` matching one sentinel, so `errors.Is` works without `T`.
  `reply`'s `whose` lists every one that names a message, or a batch for a batch handler's, to answer the requests
  it lost: add a new one there.

## Invariants

A change to channels, removal or error reporting must keep these and pass `make stress`.

- Only `inbox.release` closes `ch`, on the last reference: the subscription holds one, each send another. `acquire`
  refuses at 0, so a `Publish` racing removal skips the subscriber with a `ClosedError`.
- Nothing waits for a `Publish` but, with `WithSerial`, the next `Publish`, since `handle` may unsubscribe or
  `Close` while one waits on it. `Unsubscribe` closes `done` to free it, and `Shutdown` waits only on `Consume`.
- Every removal goes through `Group.remove` (`LoadAndDelete`, then `Subscriber.Unsubscribe`), so racing
  removals act once, and an eviction is reported once, by the error whose `remove` succeeded.
- `discarding` is set before `inbox.stop`, which may close `ch` at once.
- Only `Consume`'s goroutine reads `ch` and calls the holder. `SubscribeSeq` relays each message from it to the loop.
- `add` attaches before storing and rechecks `ctx.Err()` after, under the gate, so `Close` refuses or sees each
  subscriber.
- A batch is one send, one reference, one place in `ch` and one report, but `Stats` count its values.
- No error path the library owns may block. Only `report` calls the error handlers and `Store.Put`, outside the
  middleware chain, with the message's ctx or the subscriber's, never the `Publish`'s.
- `Publish` calls its subscriber filter, its visit, then that subscriber's own filters, one subscriber at a time in its
  goroutine: `reply` learns from the last two which subscribers a request reached.
- Each message a `Publish` picks a subscriber up for is counted once, in `Handled`, `Failed`, `TimedOut`, `Dropped`
  or `Merged`, before `report` (`TestStats_AddUp`).
- `store.File`'s positions only grow, and its sentinel table (`store/segment.go`) is part of the format: append only.

## Decisions

- The hot path stays flat: `send` tries a send before its select, which would lock the ctx's channel, keeps its selects
  in `Subscriber.send`, and `counters` take the time rather than read the clock. Measure with `make benchcmp`.
- A `Publish` without options allocates nothing in sync, buffered and non-blocking modes (`TestPublish_Allocs`).
- Async sends keep the `Publish` ctx after it returns, so that sends piling up behind a stuck subscriber can be
  cancelled.
- Connections are the user's: a subscriber reconnects in place, from `handle` or its middlewares. No middleware is
  built in.
- `Stats` cover only the subscribers still subscribed.
- Answers ride in the message (`reply.Request`), so the core knows nothing of them. `reply.Group` embeds a `Group`,
  since a method cannot add the answer's type, and only its `Publish` waits for handling, bounded by its ctx. It waits
  for the subscribers its filters mark, and answers errors from `report`, through the message's error handler, so the
  library's typed errors name the subscriber and the request.
- `middleware.FirstN`'s nil hides a loser from `reply`, so `reply/middleware.FirstN` withdraws it from the `Publish`,
  which it holds until then, since a loser may answer first. `Request` exports `Hold`, `Release` and `Withdraw` for it.
  `Race` keeps its handlers' cancels itself: `context.AfterFunc` would start a goroutine per loser.

## Tests

- Root tests (`multicast_test`) cover `subscriber` and `message` through a `Group`. Name a test after its
  option, package included: `TestSubscriberWithBuffer`, `TestMessageWithAsync`.
- Most run in a `synctest` bubble: create the Group with `newGroup(t)`, subscribe with `subscribeCtx(t)`,
  range every `SubscribeSeq`, and bound waits with `deadlockTimeout`. A goroutine blocked on a mutex hangs a bubble, so
  `pkg/gate`'s tests run in real time, like `TestStress`.
- Each example has an `Example()` needing no external service, whose output must not depend on scheduling: synchronise
  with channels, never sleep. `make tinygo-run` diffs it with `tinygo run`'s.
- The throughput benchmarks wait once, after the last `Publish`, so they loop over `b.N`, not `b.Loop`.
