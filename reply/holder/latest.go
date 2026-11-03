package holder

import (
	"time"

	"github.com/elojah/multicast/holder"
	"github.com/elojah/multicast/reply"
)

// NewLatest returns holder.NewLatest's holder for requests: of the requests a window takes, only the last reaches
// handle, and its answer is also theirs.
func NewLatest[Q, R any](d time.Duration) *holder.Reduce[reply.Request[Q, R]] {
	return holder.NewReduce(d, func(held []reply.Request[Q, R], next reply.Request[Q, R]) []reply.Request[Q, R] {
		for _, older := range held {
			next = next.Merge(older)
		}

		return append(held[:0], next)
	})
}
