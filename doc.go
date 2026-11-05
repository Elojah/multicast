// Package multicast is an in-process fan-out: Publish hands a message to every subscriber, and each subscriber
// handles its messages one at a time, in its own goroutine.
//
//	g := multicast.New[string]()
//	id, err := g.Subscribe(ctx, func(ctx context.Context, _ uint64, msg string) error {
//		fmt.Println(msg)
//		return nil
//	})
//	...
//	g.Publish(ctx, "hello")
//	...
//	err = g.Unsubscribe(ctx, id)
//
// SubscribeSeq returns an iterator instead, whose loop body replaces handle:
//
//	_, seq, err := g.SubscribeSeq(ctx)
//	...
//	for msg, fail := range seq {
//		fail(save(msg))
//	}
//
// PublishBatch hands several messages at once, with one Config: a subscriber's batch handler
// (subscriber.WithBatchHandler) gets the batch whole, and handle otherwise gets each message alone.
//
// Subscriber options are in package subscriber, and WithDefaultSubscriberOptions gives every subscriber some. Publish
// options are in package message. Packages middleware, store and filter hold ready-made middlewares, stores and filters.
// Package reply gathers the subscribers' answers to a query.
//
// # Delivery
//
// Delivery is at most once. By default Publish waits for each subscriber in turn, in subscription order, until it takes
// the message, the Publish ctx is done or the message's timeout runs out. subscriber.WithBuffer, message.WithParallel,
// message.WithAsync and message.WithNonBlocking wait less. Messages published from one goroutine arrive in order,
// except with message.WithAsync. With WithSerial, Publish calls take turns, so messages from every goroutine arrive at
// every subscriber in the same order, async sends aside.
//
// # Lifecycle
//
// Unsubscribe and Close never wait, so handle can call them. An unsubscribed subscriber still handles what it took,
// unless discarding (subscriber.WithUnsubscribeDiscard). A subscriber is also unsubscribed once its Subscribe ctx is
// done, and when subscriber.WithEvict evicts it. Shutdown is Close, then waits until every subscriber is done.
//
// # Contexts
//
// The Subscribe ctx is the subscription's lifetime, and what handle and the error handlers get, unless
// message.WithContext replaces it for one message. The Publish ctx only bounds the wait, but async sends keep using
// it after Publish returns.
//
// # Errors
//
// Every error about a message goes to the error handlers, or is discarded: handle's as is, a missed message's as one of
// package subscriber's error types, each matching a sentinel with errors.Is. subscriber.WithDeadLetters also stores
// every message the subscriber loses.
package multicast
