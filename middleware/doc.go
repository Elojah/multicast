// Package middleware holds ready-made subscriber.Middleware for subscriber.WithMiddleware, in the recommended order:
//
//	id, err := g.Subscribe(ctx, handle, subscriber.WithMiddleware(
//		middleware.Recover[string](),
//		middleware.History[string](history),
//		middleware.MaxAge[string](time.Minute, sentAt),
//		middleware.WrapError[string](),
//		middleware.Retry[string](middleware.RetryPolicy{Attempts: 3, Delay: 10 * time.Millisecond}),
//	))
//
// Recover first catches panics in every later middleware, which Retry then never retries. FirstN goes right after it,
// so that History records no loser that has not handled the message and Retry stops with the race. History records only
// handled messages, and its failed Put is neither wrapped nor retried, nor is an expired message. Retry after WrapError
// retries handle's raw errors, so only the last one is wrapped.
//
// # Under a reply.Group
//
// Warning: use FirstN from package reply/middleware, not this package's, which returns a loser's answer as if it had
// won, or from multicast.WithDefaultSubscriberOptions holds Publish until ctx is done. Likewise, use NewLatest from
// package reply/holder, not package holder's, which holds until ctx is done the Publish of each request it drops. The
// other middlewares work as they are.
package middleware
