package holder

import (
	"cmp"
	"time"

	"github.com/elojah/multicast/holder"
	"github.com/elojah/multicast/reply"
)

// NewReorder returns holder.NewReorder's holder for requests: once a window ends, its requests reach handle in
// ascending order of their Query.
func NewReorder[Q cmp.Ordered, R any](d time.Duration) *holder.Reorder[reply.Request[Q, R]] {
	return holder.NewReorderFunc(d, byQuery[Q, R])
}

// byQuery compares requests by their Query.
func byQuery[Q cmp.Ordered, R any](a, b reply.Request[Q, R]) int {
	return cmp.Compare(a.Query, b.Query)
}
