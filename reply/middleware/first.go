package middleware

import (
	"context"

	"github.com/elojah/multicast/middleware"
	"github.com/elojah/multicast/reply"
	"github.com/elojah/multicast/subscriber"
)

// FirstN is package middleware's FirstN for a reply.Group, for each query race returns a Race for: a subscriber that
// loses leaves the Publish, whatever it answered, so that Publish returns the winners and those that failed before the
// race ended. It also works from multicast.WithDefaultSubscriberOptions and with SubscribeSeq:
//
//	racing := subscriber.WithMiddleware(replymiddleware.FirstN[Q, R](1, func(q Q) *middleware.Race { return q.race }))
//	...
//	race := middleware.NewRace()
//	defer race.Stop()
//	results := g.Publish(ctx, Q{race: race})
//
// Publish waits for each loser to stop, so until ctx is done for one ignoring its ctx or busy with another request.
// Race.Stop ends a Retry inside FirstN that would hold up the next Publish. Do not end Publish's ctx on Race.Done,
// which closes before the last winner answers.
func FirstN[Q, R any](n int, race func(query Q) *middleware.Race) subscriber.Middleware[reply.Request[Q, R]] {
	first := middleware.FirstN(n, func(req reply.Request[Q, R]) *middleware.Race { return race(req.Query) })

	return func(next subscriber.Handler[reply.Request[Q, R]]) subscriber.Handler[reply.Request[Q, R]] {
		racing := first(next)

		return func(ctx context.Context, id uint64, req reply.Request[Q, R]) error {
			r := race(req.Query)
			if r == nil {
				return racing(ctx, id, req)
			}
			// A loser may answer before it loses: Publish waits to know, so that it never returns that answer.
			req.Hold()
			defer req.Release()

			err := racing(ctx, id, req)
			if err == nil && !r.Won(id) {
				req.Withdraw(id)
			}

			return err
		}
	}
}
