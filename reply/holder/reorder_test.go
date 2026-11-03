package holder_test

import (
	"context"
	"maps"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast/reply/holder"
	"github.com/elojah/multicast/subscriber"
)

// The requests of a window reach handle in order of their Query once it ends, each answered on its own.
func TestReorder(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		var handled []string // Only the subscriber's goroutine appends, before answering.
		id := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) {
			handled = append(handled, query)

			return len(query), nil
		}, subscriber.WithHolder(holder.NewReorder[string, int](time.Second)))

		got, elapsed := publishAll(t, g, "south-west", "north")()
		for i, want := range []results{{id: {Value: 10}}, {id: {Value: 5}}} {
			if !maps.Equal(got[i], want) || elapsed[i] != time.Second {
				t.Errorf("Publish %d returned %v after %v, want %v after %v", i, got[i], elapsed[i], want, time.Second)
			}
		}
		if want := []string{"north", "south-west"}; !slices.Equal(handled, want) {
			t.Errorf("handle got %v, want %v", handled, want)
		}
	})
}
