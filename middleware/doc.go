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
// Recover first catches panics in every later middleware, which Retry then never retries. FirstN, for subscribers
// racing for a message, goes right after it, so that History never records a loser and Retry stops once the race is
// over. History records only handled messages, and its failed Put is neither wrapped nor retried, nor is an expired
// message. Retry after WrapError retries handle's raw errors, so only the last one is wrapped.
package middleware
