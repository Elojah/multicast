package holder_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/reply/holder"
)

// BenchmarkLatest_Merge measures NewLatest merging the requests of a window into the last one, then the answer to that
// one reaching them all, per request.
func BenchmarkLatest_Merge(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("requests=%d", n), func(b *testing.B) {
			h := holder.NewLatest[string, int](time.Second)
			now := time.Now()
			m := message.Message[ask]{Value: ask{Query: "north"}}

			b.ReportAllocs()
			for b.Loop() {
				for range n {
					h.Hold(m, now)
				}
				released, _, ok := h.Release(now, true)
				if !ok {
					b.Fatal("Release returned nothing")
				}
				released.Value.Answer(1, len(released.Value.Query), nil)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/request")
		})
	}
}
