package holder

import (
	"cmp"
	"time"

	"github.com/elojah/multicast/holder"
	"github.com/elojah/multicast/reply"
)

// NewJitter returns holder.NewJitter's holder for requests: each one is held for d, and leaves after those held with a
// lesser Query.
func NewJitter[Q cmp.Ordered, R any](d time.Duration) *holder.Jitter[reply.Request[Q, R]] {
	return holder.NewJitterFunc(d, byQuery[Q, R])
}
