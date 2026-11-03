// Package middleware holds ready-made subscriber.Middleware for reply's requests:
//
//	id, err := g.Subscribe(ctx, handle, subscriber.WithMiddleware(middleware.FirstN[string, float64](1, raceOf)))
//
// FirstN replaces package middleware's. The others of that package take requests as they are.
package middleware
