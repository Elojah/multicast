package multicast_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/subscriber"
)

// A filtered message never reaches the subscriber: Publish neither waits for it, stuck as it is, nor counts it, and
// it is neither reported nor in Stats.
func TestSubscriberWithFilter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		stuck := &recorder[int]{hold: make(chan struct{})}
		id := subscribe(t, g, stuck.handle, subscriber.WithFilter(even), subscriber.WithTimeout[int](time.Second),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) {
				t.Errorf("error handler got %v, want none", err)
			}))

		g.Publish(t.Context(), 2) // stuck holds on to 2
		start := time.Now()
		for _, msg := range []int{1, 3} {
			if n := g.Publish(t.Context(), msg); n != 0 {
				t.Errorf("Publish handed %d, which the filter rejects, to %d subscribers, want 0", msg, n)
			}
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Publish calls of rejected messages returned after %v, want no wait", elapsed)
		}
		synctest.Wait()
		checkStats(t, g, id, subscriber.Stats{Delivered: 1})

		stuck.release()
		if n := g.Publish(t.Context(), 4); n != 1 {
			t.Errorf("Publish handed 4 to %d subscribers, want 1", n)
		}
		synctest.Wait()
		checkStats(t, g, id, subscriber.Stats{Delivered: 2, Handled: 2})
		unsubscribe(t, g, id)
		synctest.Wait()

		if got, want := stuck.messages(), []int{2, 4}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
	})
}

// A message must pass every filter, in order, so each sees only what those before it kept. A nil filter keeps
// everything.
func TestSubscriberWithFilter_Several(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		seen := &recorder[int]{}
		r := &recorder[int]{}
		id := subscribe(t, g, r.handle,
			subscriber.WithFilter(even),
			subscriber.WithFilter[int](nil),
			subscriber.WithFilter(func(msg int) bool {
				seen.record(msg)

				return msg > 2
			}))

		for msg := 1; msg <= 6; msg++ {
			g.Publish(t.Context(), msg)
		}
		unsubscribe(t, g, id)
		synctest.Wait()

		if got, want := seen.messages(), []int{2, 4, 6}; !slices.Equal(got, want) {
			t.Errorf("second filter got %v, want %v", got, want)
		}
		if got, want := r.messages(), []int{4, 6}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
	})
}

// A Publish hands its message only to the subscribers it picks: it neither waits for the others, stuck as they are,
// nor counts them, and nothing about them is reported or in their Stats.
func TestMessageWithSubscriberFilter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		stuck := &recorder[int]{hold: make(chan struct{})}
		stuckID := subscribe(t, g, stuck.handle, subscriber.WithTimeout[int](time.Second),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) {
				t.Errorf("error handler got %v, want none", err)
			}))
		picked, pickedID := record(t, g)
		only := message.WithSubscriberFilter[int](func(id uint64) bool { return id == pickedID })

		g.Publish(t.Context(), 0) // stuck holds on to 0
		start := time.Now()
		for _, msg := range []int{1, 2} {
			if n := g.Publish(t.Context(), msg, only); n != 1 {
				t.Errorf("Publish handed %d to %d subscribers, want 1: the one picked", msg, n)
			}
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Publish calls skipping the stuck subscriber returned after %v, want no wait", elapsed)
		}
		synctest.Wait()
		checkStats(t, g, stuckID, subscriber.Stats{Delivered: 1})

		stuck.release()
		unsubscribeAll(t, g, stuckID, pickedID)
		synctest.Wait()

		if got, want := stuck.messages(), []int{0}; !slices.Equal(got, want) {
			t.Errorf("stuck subscriber got %v, want %v", got, want)
		}
		if got, want := picked.messages(), []int{0, 1, 2}; !slices.Equal(got, want) {
			t.Errorf("picked subscriber got %v, want %v", got, want)
		}
	})
}

// A subscriber's own filters never see a message the Publish skips it for, so a stateful one only learns what the
// subscriber was handed.
func TestMessageWithSubscriberFilter_BeforeSubscriberFilter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		seen := &recorder[int]{}
		r := &recorder[int]{}
		id := subscribe(t, g, r.handle, subscriber.WithFilter(func(msg int) bool {
			seen.record(msg)

			return true
		}))

		if n := g.Publish(t.Context(), 1, message.WithSubscriberFilter[int](func(uint64) bool { return false })); n != 0 {
			t.Errorf("Publish picking no subscriber handed the message to %d, want 0", n)
		}
		g.Publish(t.Context(), 2)
		unsubscribe(t, g, id)
		synctest.Wait()

		if got, want := seen.messages(), []int{2}; !slices.Equal(got, want) {
			t.Errorf("subscriber's filter got %v, want %v", got, want)
		}
		if got, want := r.messages(), []int{2}; !slices.Equal(got, want) {
			t.Errorf("handle got %v, want %v", got, want)
		}
	})
}

// A subscriber is picked only if every filter picks it, in order, so each sees only the subscribers those before it
// picked. A nil filter picks every one.
func TestMessageWithSubscriberFilter_Several(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		first, firstID := record(t, g)
		second, secondID := record(t, g)
		third, thirdID := record(t, g)
		// Only Publish's goroutine calls the filters.
		var seen []uint64

		n := g.Publish(t.Context(), 1,
			message.WithSubscriberFilter[int](func(id uint64) bool { return id != firstID }),
			message.WithSubscriberFilter[int](nil),
			message.WithSubscriberFilter[int](func(id uint64) bool {
				seen = append(seen, id)

				return id != secondID
			}))
		if n != 1 {
			t.Errorf("Publish handed the message to %d subscribers, want 1", n)
		}
		unsubscribeAll(t, g, firstID, secondID, thirdID)
		synctest.Wait()

		if len(seen) != 2 || !slices.Contains(seen, secondID) || !slices.Contains(seen, thirdID) {
			t.Errorf("last filter got %v, want %v and %v, in any order", seen, secondID, thirdID)
		}
		for _, tt := range []struct {
			name string
			r    *recorder[int]
			want []int
		}{
			{name: "first", r: first},
			{name: "second", r: second},
			{name: "third", r: third, want: []int{1}},
		} {
			if got := tt.r.messages(); !slices.Equal(got, tt.want) {
				t.Errorf("%s subscriber got %v, want %v", tt.name, got, tt.want)
			}
		}
	})
}

// A Publish visits each subscriber its subscriber filter picks, once, before that subscriber's own filters, whether
// they keep the message or not. Several visits run in order, and a nil one is ignored.
func TestMessageWithVisit(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		// What each subscriber went through, in order. Only Publish's goroutine calls the visits and the filters.
		calls := map[uint64][]string{}
		_, skippedID := record(t, g)
		var rejectingID uint64
		rejecting := &recorder[int]{}
		rejectingID = subscribe(t, g, rejecting.handle, subscriber.WithFilter(func(int) bool {
			calls[rejectingID] = append(calls[rejectingID], "filter")

			return false
		}))
		kept, keptID := record(t, g)

		n := g.Publish(t.Context(), 1,
			message.WithSubscriberFilter[int](func(id uint64) bool { return id != skippedID }),
			message.WithVisit[int](func(id uint64) { calls[id] = append(calls[id], "first") }),
			message.WithVisit[int](nil),
			message.WithVisit[int](func(id uint64) { calls[id] = append(calls[id], "second") }))
		if n != 1 {
			t.Errorf("Publish handed the message to %d subscribers, want 1", n)
		}
		unsubscribeAll(t, g, skippedID, rejectingID, keptID)
		synctest.Wait()

		want := map[uint64][]string{
			rejectingID: {"first", "second", "filter"},
			keptID:      {"first", "second"},
		}
		if !maps.EqualFunc(calls, want, slices.Equal) {
			t.Errorf("Publish called %v, want %v", calls, want)
		}
		if got := rejecting.messages(); len(got) != 0 {
			t.Errorf("rejecting subscriber got %v, want none", got)
		}
		if got, want := kept.messages(), []int{1}; !slices.Equal(got, want) {
			t.Errorf("kept subscriber got %v, want %v", got, want)
		}
	})
}

// A Publish with a limit stops once that many subscribers took the message, in subscription order: one whose filter
// rejects it, or that misses it, hands over to the next, and those after the last are neither visited, reported nor
// counted.
func TestMessageWithLimit(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		errs := &recorder[error]{}
		report := subscriber.WithErrorHandler[int](func(_ context.Context, err error) { errs.record(err) })
		offline := &recorder[int]{}
		offlineID := subscribe(t, g, offline.handle, report, subscriber.WithFilter(func(int) bool { return false }))
		busy := &recorder[int]{hold: make(chan struct{})}
		busyID := subscribe(t, g, busy.handle, report)
		first, second, last := &recorder[int]{}, &recorder[int]{}, &recorder[int]{}
		firstID := subscribe(t, g, first.handle, report)
		secondID := subscribe(t, g, second.handle, report)
		lastID := subscribe(t, g, last.handle, report)

		// busy holds on to 0.
		g.Publish(t.Context(), 0, message.WithSubscriberFilter[int](func(id uint64) bool { return id == busyID }))
		synctest.Wait()
		// Only Publish's goroutine calls the visit.
		var visited []uint64
		n := g.Publish(t.Context(), 1, message.WithNonBlocking[int](), message.WithLimit[int](2),
			message.WithVisit[int](func(id uint64) { visited = append(visited, id) }))
		synctest.Wait()

		if n != 2 {
			t.Errorf("Publish handed the message to %d subscribers, want 2", n)
		}
		if want := []uint64{offlineID, busyID, firstID, secondID}; !slices.Equal(visited, want) {
			t.Errorf("Publish visited %v, want %v", visited, want)
		}
		got := errs.messages()
		var dropped *subscriber.DroppedError[int]
		if len(got) != 1 || !errors.As(got[0], &dropped) || dropped.SubscriberID != busyID || dropped.Message != 1 {
			t.Errorf("error handlers got %v, want a single *DroppedError for subscriber %d and message 1", got, busyID)
		}
		checkStats(t, g, lastID, subscriber.Stats{})

		busy.release()
		unsubscribeAll(t, g, offlineID, busyID, firstID, secondID, lastID)
		synctest.Wait()
		for _, tt := range []struct {
			name string
			r    *recorder[int]
			want []int
		}{
			{name: "offline", r: offline},
			{name: "busy", r: busy, want: []int{0}},
			{name: "first", r: first, want: []int{1}},
			{name: "second", r: second, want: []int{1}},
			{name: "last", r: last},
		} {
			if got := tt.r.messages(); !slices.Equal(got, tt.want) {
				t.Errorf("%s subscriber got %v, want %v", tt.name, got, tt.want)
			}
		}
	})
}

// A limit counts subscribers, a batch's included, and parallel or async sends once started. 0 or less, the last
// WithLimit's, means none, and so does a subscriber's default.
func TestMessageWithLimit_Count(t *testing.T) {
	t.Parallel()

	const subscribers = 3
	for _, tt := range []struct {
		name      string
		subscribe []subscriber.Option[int]
		publish   []message.Option[int]
		batch     bool
		want      int
	}{
		{name: "sync", publish: []message.Option[int]{message.WithLimit[int](2)}, want: 2},
		{name: "parallel", publish: []message.Option[int]{message.WithParallel[int](), message.WithLimit[int](2)}, want: 2},
		{name: "async", publish: []message.Option[int]{message.WithAsync[int](), message.WithLimit[int](2)}, want: 2},
		{
			name: "non-blocking", subscribe: []subscriber.Option[int]{subscriber.WithBuffer[int](1)},
			publish: []message.Option[int]{message.WithNonBlocking[int](), message.WithLimit[int](2)}, want: 2,
		},
		{name: "batch", publish: []message.Option[int]{message.WithLimit[int](2)}, batch: true, want: 2},
		{name: "above subscribers", publish: []message.Option[int]{message.WithLimit[int](5)}, want: subscribers},
		{name: "zero", publish: []message.Option[int]{message.WithLimit[int](0)}, want: subscribers},
		{name: "negative", publish: []message.Option[int]{message.WithLimit[int](-1)}, want: subscribers},
		{
			name:    "last wins",
			publish: []message.Option[int]{message.WithLimit[int](1), message.WithLimit[int](0)}, want: subscribers,
		},
		{
			name:      "default ignored",
			subscribe: []subscriber.Option[int]{subscriber.WithDefaultMessageOptions(message.WithLimit[int](1))},
			want:      subscribers,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := newGroup[int](t)
				rs := make([]*recorder[int], subscribers)
				ids := make([]uint64, subscribers)
				for i := range rs {
					rs[i] = &recorder[int]{}
					ids[i] = subscribe(t, g, rs[i].handle, tt.subscribe...)
				}

				var n int
				if tt.batch {
					n = g.PublishBatch(t.Context(), batchOf(1), tt.publish...)
				} else {
					n = g.Publish(t.Context(), 1, tt.publish...)
				}
				// So that async sends are taken before Unsubscribe.
				synctest.Wait()
				unsubscribeAll(t, g, ids...)
				synctest.Wait()

				if n != tt.want {
					t.Errorf("Publish handed the message to %d subscribers, want %d", n, tt.want)
				}
				for i, r := range rs {
					var want []int
					if i < tt.want {
						want = []int{1}
					}
					if got := r.messages(); !slices.Equal(got, want) {
						t.Errorf("subscriber %d got %v, want %v", i, got, want)
					}
				}
			})
		})
	}
}

func even(msg int) bool {
	return msg%2 == 0
}
