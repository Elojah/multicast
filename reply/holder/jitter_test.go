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

// Each request is held for d from when it came, and once one is due, those ordered before it by Query leave first.
func TestJitter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		var handled []string // Only the subscriber's goroutine appends, before answering.
		id := subscribe(t, g, func(_ context.Context, _ uint64, query string) (int, error) {
			handled = append(handled, query)

			return len(query), nil
		}, subscriber.WithHolder(holder.NewJitter[string, int](time.Second)))

		first := publishAll(t, g, "south-west")
		time.Sleep(time.Second / 2)
		second := publishAll(t, g, "north")
		gotFirst, elapsedFirst := first()
		gotSecond, elapsedSecond := second()
		// north, ordered first, leaves once south-west is due, half a second after it came.
		if want := (results{id: {Value: 10}}); !maps.Equal(gotFirst[0], want) || elapsedFirst[0] != time.Second {
			t.Errorf("Publish of south-west returned %v after %v, want %v after %v", gotFirst[0], elapsedFirst[0], want, time.Second)
		}
		if want := (results{id: {Value: 5}}); !maps.Equal(gotSecond[0], want) || elapsedSecond[0] != time.Second/2 {
			t.Errorf("Publish of north returned %v after %v, want %v after %v", gotSecond[0], elapsedSecond[0], want, time.Second/2)
		}
		if want := []string{"north", "south-west"}; !slices.Equal(handled, want) {
			t.Errorf("handle got %v, want %v", handled, want)
		}
	})
}
