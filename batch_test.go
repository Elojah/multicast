package multicast_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/elojah/multicast/holder"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/subscriber"
)

// A subscriber hands a batch whole to its batch handler, through the batch middlewares, and each single message to
// handle. One without a batch handler hands each value to handle, through its middlewares. Stats count values.
func TestPublishBatch(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		unrolled, wrapped := &recorder[int]{}, &recorder[int]{}
		unrolledID := subscribe(t, g, unrolled.handle, subscriber.WithMiddleware(recordMiddleware(wrapped)))
		singles, batches := &recorder[int]{}, &batchRecorder{}
		sent := batchOf(1, 2, 3)
		var calls atomic.Int64
		batchID := subscribe(t, g, singles.handle, subscriber.WithBatchHandler(
			func(ctx context.Context, id uint64, batch message.Batch[int]) error {
				if batch != message.Batch[int](sent) {
					t.Errorf("batch handler got %v, want the batch sent", batch)
				}

				return batches.handle(ctx, id, batch)
			},
			func(next subscriber.Handler[message.Batch[int]]) subscriber.Handler[message.Batch[int]] {
				return func(ctx context.Context, id uint64, batch message.Batch[int]) error {
					calls.Add(1)

					return next(ctx, id, batch)
				}
			}))

		if n := g.PublishBatch(t.Context(), sent); n != 2 {
			t.Errorf("PublishBatch handed the batch to %d subscribers, want 2", n)
		}
		if n := g.Publish(t.Context(), 4); n != 2 {
			t.Errorf("Publish handed the message to %d subscribers, want 2", n)
		}
		for _, empty := range []message.Batch[int]{nil, batchOf()} {
			if n := g.PublishBatch(t.Context(), empty); n != 0 {
				t.Errorf("PublishBatch(%v) handed it to %d subscribers, want none", empty, n)
			}
		}
		synctest.Wait()

		want := []int{1, 2, 3, 4}
		if got := unrolled.messages(); !slices.Equal(got, want) {
			t.Errorf("handle without a batch handler got %v, want %v", got, want)
		}
		if got := wrapped.messages(); !slices.Equal(got, want) {
			t.Errorf("its middleware got %v, want %v", got, want)
		}
		if got := singles.messages(); !slices.Equal(got, []int{4}) {
			t.Errorf("handle with a batch handler got %v, want [4]", got)
		}
		checkBatches(t, batches, [][]int{{1, 2, 3}})
		if n := calls.Load(); n != 1 {
			t.Errorf("the batch middleware ran %d times, want once", n)
		}
		checkStats(t, g, unrolledID, subscriber.Stats{Delivered: 4, Handled: 4})
		checkStats(t, g, batchID, subscriber.Stats{Delivered: 4, Handled: 4, Batches: 1})
	})
}

// A batch the subscriber cannot take is lost whole: reported once, with the batch in the error, to the subscriber's
// error handler then to the batch's, and stored one value at a time, each with that error. Stats count each value.
func TestPublishBatch_Lost(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		handled := &recorder[int]{hold: make(chan struct{})}
		var subscriberErrs, batchErrs []string
		store := &recordStore{}
		id := subscribe(t, g, handled.handle, subscriber.WithDeadLetters[int](store),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) {
				subscriberErrs = append(subscriberErrs, describeBatchLoss(t, err))
			}))
		recordBatchErrs := message.WithErrorHandler[int](func(_ context.Context, err error) {
			batchErrs = append(batchErrs, describeBatchLoss(t, err))
		})

		// handle holds on to 0, so the batches find it busy.
		g.Publish(t.Context(), 0)
		synctest.Wait()
		if n := g.PublishBatch(t.Context(), batchOf(1, 2, 3), message.WithNonBlocking[int](), recordBatchErrs); n != 0 {
			t.Errorf("non-blocking PublishBatch to a busy subscriber handed it over %d times, want none", n)
		}
		if n := g.PublishBatch(t.Context(), batchOf(4, 5), message.WithTimeout[int](time.Second), recordBatchErrs); n != 0 {
			t.Errorf("PublishBatch timing out handed it over %d times, want none", n)
		}
		checkStats(t, g, id, subscriber.Stats{Delivered: 1, TimedOut: 2, Dropped: 3, Handling: time.Second})
		handled.release()
		synctest.Wait()

		want := []string{"dropped [1 2 3]", "timed out [4 5]"}
		if !slices.Equal(subscriberErrs, want) || !slices.Equal(batchErrs, want) {
			t.Errorf("the subscriber's error handler got %v and the batch's %v, want %v for both", subscriberErrs, batchErrs, want)
		}
		records := store.messages()
		stored := make([]int, 0, len(records))
		for _, r := range records {
			stored = append(stored, r.Message)
			if wantErr := map[bool]error{true: subscriber.ErrDropped, false: subscriber.ErrTimeout}[r.Message <= 3]; !errors.Is(r.Err, wantErr) {
				t.Errorf("record for %d has error %v, want one matching %v", r.Message, r.Err, wantErr)
			}
		}
		if wantStored := []int{1, 2, 3, 4, 5}; !slices.Equal(stored, wantStored) {
			t.Errorf("dead letters got %v, want %v", stored, wantStored)
		}
		checkStats(t, g, id, subscriber.Stats{Delivered: 1, Handled: 1, TimedOut: 2, Dropped: 3, HandleTime: time.Second})
	})
}

// The batch handler gets the batch's ctx. Its error counts each value as failed, and is reported once, to the
// subscriber's error handler then to the batch's, with the batch's ctx, and stored one value at a time.
func TestSubscriberWithBatchHandler_Errors(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		type key struct{}
		var handledWith any
		var subscriberErrs, batchErrs []error
		store := &recordStore{}
		id := subscribe(t, g, (&recorder[int]{}).handle, subscriber.WithDeadLetters[int](store),
			subscriber.WithBatchHandler(func(ctx context.Context, _ uint64, batch message.Batch[int]) error {
				handledWith = ctx.Value(key{})

				return handleError(batch.Len())
			}),
			subscriber.WithErrorHandler[int](func(ctx context.Context, err error) {
				if v := ctx.Value(key{}); v != "batch" {
					t.Errorf("the subscriber's error handler got a ctx with %v, want the batch's", v)
				}
				subscriberErrs = append(subscriberErrs, err)
			}))

		g.PublishBatch(t.Context(), batchOf(1, 2, 3), message.WithContext[int](context.WithValue(t.Context(), key{}, "batch")),
			message.WithErrorHandler[int](func(_ context.Context, err error) { batchErrs = append(batchErrs, err) }))
		synctest.Wait()

		if handledWith != "batch" {
			t.Errorf("the batch handler got a ctx with %v, want the batch's", handledWith)
		}
		want := []error{handleError(3)}
		if !slices.Equal(subscriberErrs, want) || !slices.Equal(batchErrs, want) {
			t.Errorf("the subscriber's error handler got %v and the batch's %v, want %v for both", subscriberErrs, batchErrs, want)
		}
		wantRecords := []subscriber.Record[int]{
			{SubscriberID: id, Message: 1, Err: handleError(3)},
			{SubscriberID: id, Message: 2, Err: handleError(3)},
			{SubscriberID: id, Message: 3, Err: handleError(3)},
		}
		if got := store.messages(); !slices.Equal(got, wantRecords) {
			t.Errorf("dead letters got %v, want %v", got, wantRecords)
		}
		checkStats(t, g, id, subscriber.Stats{Delivered: 3, Failed: 3, Batches: 1})
	})
}

// WithBatchMiddleware wraps the batch handler and its own middlewares, outermost and in order, whichever option comes
// first, and what it returns is reported. Without a batch handler, it never runs.
func TestSubscriberWithBatchMiddleware(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		calls := &recorder[string]{}
		named := func(name string) subscriber.Middleware[message.Batch[int]] {
			return func(next subscriber.Handler[message.Batch[int]]) subscriber.Handler[message.Batch[int]] {
				return func(ctx context.Context, id uint64, batch message.Batch[int]) error {
					calls.record(name)
					if err := next(ctx, id, batch); err != nil {
						return fmt.Errorf("%s: %w", name, err)
					}

					return nil
				}
			}
		}
		var reported []error
		batched := subscribe(t, g, (&recorder[int]{}).handle,
			subscriber.WithBatchMiddleware(named("first"), named("second")),
			subscriber.WithBatchHandler(func(_ context.Context, _ uint64, batch message.Batch[int]) error {
				calls.record("handler")

				return handleError(batch.Len())
			}, named("own")),
			subscriber.WithBatchMiddleware(named("third")),
			subscriber.WithErrorHandler[int](func(_ context.Context, err error) { reported = append(reported, err) }))
		unbatched := &recorder[int]{}
		subscribe(t, g, unbatched.handle, subscriber.WithBatchMiddleware(named("unused")))

		g.PublishBatch(t.Context(), batchOf(1, 2))
		synctest.Wait()

		if got, want := calls.messages(), []string{"first", "second", "third", "own", "handler"}; !slices.Equal(got, want) {
			t.Errorf("calls went %v, want %v", got, want)
		}
		if want := "first: second: third: own: " + handleError(2).Error(); len(reported) != 1 || reported[0].Error() != want ||
			!errors.Is(reported[0], handleError(2)) {
			t.Errorf("error handler got %v, want one error %q wrapping %v", reported, want, handleError(2))
		}
		if got, want := unbatched.messages(), []int{1, 2}; !slices.Equal(got, want) {
			t.Errorf("handle without a batch handler got %v, want %v", got, want)
		}
		checkStats(t, g, batched, subscriber.Stats{Delivered: 2, Failed: 2, Batches: 1})
	})
}

// A subscriber's filter sees each value of a batch once. Of a batch it keeps part of, the subscriber takes a batch of
// the same type holding what it keeps, of one it keeps whole, the batch itself, and of one it keeps nothing of,
// nothing, neither counted nor reported.
func TestPublishBatch_Filter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		var filtered atomic.Int64
		keepEven := subscriber.WithFilter(func(msg int) bool {
			filtered.Add(1)

			return msg%2 == 0
		})
		var got []message.Batch[int]
		batchID := subscribe(t, g, (&recorder[int]{}).handle, keepEven, subscriber.WithBatchHandler(
			func(_ context.Context, _ uint64, batch message.Batch[int]) error {
				got = append(got, batch)

				return nil
			}), subscriber.WithErrorHandler[int](func(_ context.Context, err error) {
			t.Errorf("error handler got %v, want nothing", err)
		}))
		unrolled := &recorder[int]{}
		unrolledID := subscribe(t, g, unrolled.handle, subscriber.WithFilter(func(msg int) bool { return msg%2 == 0 }))

		part, whole, none := &tally{values: []int{1, 2, 3, 4}}, &tally{values: []int{6, 8}}, &tally{values: []int{5, 7}}
		for _, tt := range []struct {
			batch *tally
			want  int
		}{{part, 2}, {whole, 2}, {none, 0}} {
			if n := g.PublishBatch(t.Context(), tt.batch); n != tt.want {
				t.Errorf("PublishBatch(%v) handed it to %d subscribers, want %d", tt.batch.values, n, tt.want)
			}
		}
		synctest.Wait()

		if n := filtered.Load(); n != 8 {
			t.Errorf("the filter ran %d times, want 8, once per value", n)
		}
		if len(got) != 2 {
			t.Fatalf("batch handler got %d batches, want 2", len(got))
		}
		if kept, ok := got[0].(*tally); !ok || kept == part || !slices.Equal(kept.values, []int{2, 4}) {
			t.Errorf("batch handler got %#v first, want a new *tally holding [2 4]", got[0])
		}
		if got[1] != message.Batch[int](whole) {
			t.Errorf("batch handler got %#v second, want the batch sent", got[1])
		}
		if got, want := unrolled.messages(), []int{2, 4, 6, 8}; !slices.Equal(got, want) {
			t.Errorf("handle without a batch handler got %v, want %v", got, want)
		}
		checkStats(t, g, batchID, subscriber.Stats{Delivered: 4, Handled: 4, Batches: 2})
		checkStats(t, g, unrolledID, subscriber.Stats{Delivered: 4, Handled: 4})
	})
}

// A Map holds each value Add gives it under a key of its own. A batch handler gets it whole, or, of one its filter keeps
// part of, a new Map holding what it keeps, and a subscriber without one gets each value, in no particular order.
func TestPublishBatch_Map(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		var whole, part []message.Batch[int]
		subscribe(t, g, (&recorder[int]{}).handle, subscriber.WithBatchHandler(
			func(_ context.Context, _ uint64, batch message.Batch[int]) error {
				whole = append(whole, batch)

				return nil
			}))
		subscribe(t, g, (&recorder[int]{}).handle, subscriber.WithFilter(func(msg int) bool { return msg%2 == 0 }),
			subscriber.WithBatchHandler(func(_ context.Context, _ uint64, batch message.Batch[int]) error {
				part = append(part, batch)

				return nil
			}))
		unrolled := &recorder[int]{}
		subscribe(t, g, unrolled.handle)

		sent := new(message.Map[int])
		for msg := range 4 {
			sent.Add(msg + 1)
		}
		if n := sent.Len(); n != 4 {
			t.Fatalf("Map holds %d values after 4 Adds, want 4", n)
		}
		if n := g.PublishBatch(t.Context(), sent); n != 3 {
			t.Errorf("PublishBatch handed the Map to %d subscribers, want 3", n)
		}
		synctest.Wait()

		if len(whole) != 1 || whole[0] != message.Batch[int](sent) {
			t.Errorf("batch handler got %v, want the Map sent", whole)
		}
		if len(part) != 1 {
			t.Fatalf("filtered batch handler got %d batches, want 1", len(part))
		}
		if kept, ok := part[0].(*message.Map[int]); !ok || kept == sent || !slices.Equal(slices.Sorted(kept.All()), []int{2, 4}) {
			t.Errorf("filtered batch handler got %#v, want a new *Map holding [2 4]", part[0])
		}
		if got, want := slices.Sorted(slices.Values(unrolled.messages())), []int{1, 2, 3, 4}; !slices.Equal(got, want) {
			t.Errorf("handle without a batch handler got %v, want %v in any order", got, want)
		}
	})
}

// A batch the subscriber discards while it waits in the buffer is reported whole, once. Once handed to handle one
// value at a time, what is left is reported a value at a time.
func TestPublishBatch_Discard(t *testing.T) {
	t.Parallel()

	t.Run("buffered", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			g := newGroup[int](t)
			handled := &recorder[int]{hold: make(chan struct{})}
			var errs []string
			id := subscribe(t, g, handled.handle, subscriber.WithBuffer[int](1),
				subscriber.WithErrorHandler[int](func(_ context.Context, err error) { errs = append(errs, describeBatchLoss(t, err)) }))
			g.Publish(t.Context(), 0)
			synctest.Wait()
			g.PublishBatch(t.Context(), batchOf(1, 2, 3))
			checkStats(t, g, id, subscriber.Stats{Delivered: 4, Queued: 3, Buffer: 1})

			if err := g.Unsubscribe(t.Context(), id, subscriber.WithUnsubscribeDiscard()); err != nil {
				t.Fatalf("Unsubscribe: %v", err)
			}
			handled.release()
			synctest.Wait()

			if got := handled.messages(); !slices.Equal(got, []int{0}) {
				t.Errorf("handle got %v, want [0]", got)
			}
			if want := []string{"closed [1 2 3]"}; !slices.Equal(errs, want) {
				t.Errorf("error handler got %v, want %v", errs, want)
			}
		})
	})

	t.Run("handed over", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			g := newGroup[int](t)
			handled, closed := &recorder[int]{}, &recorder[int]{}
			subscribe(t, g, func(ctx context.Context, id uint64, msg int) error {
				handled.record(msg)
				if err := g.Unsubscribe(ctx, id, subscriber.WithUnsubscribeDiscard()); err != nil {
					t.Errorf("Unsubscribe: %v", err)
				}

				return nil
			}, subscriber.WithErrorHandler[int](recordClosed(t, closed)))
			g.PublishBatch(t.Context(), batchOf(1, 2, 3))
			synctest.Wait()

			if got := handled.messages(); !slices.Equal(got, []int{1}) {
				t.Errorf("handle got %v, want [1]", got)
			}
			if got := closed.messages(); !slices.Equal(got, []int{2, 3}) {
				t.Errorf("error handler got *subscriber.ClosedError for %v, want [2 3]", got)
			}
		})
	})
}

// holder.NewBatch gathers what the subscriber takes, singles and batches' values alike, into batches of n, released
// before the next value is held, then what is left once its window ends, or at Shutdown. Stats count values held.
func TestSubscriberWithHolder_Batch(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := newGroup[int](t)
		singles, batches := &recorder[int]{}, &batchRecorder{}
		id := subscribe(t, g, singles.handle, subscriber.WithHolder(holder.NewBatch[int](holdFor, 3)),
			subscriber.WithBatchHandler(batches.handle))

		g.Publish(t.Context(), 1)
		g.Publish(t.Context(), 2)
		g.PublishBatch(t.Context(), batchOf(3, 4, 5, 6, 7))
		synctest.Wait()
		checkBatches(t, batches, [][]int{{1, 2, 3}, {4, 5, 6}})
		checkStats(t, g, id, subscriber.Stats{Delivered: 7, Handled: 6, Held: 1, Batches: 2})

		time.Sleep(holdFor)
		synctest.Wait()
		checkBatches(t, batches, [][]int{{1, 2, 3}, {4, 5, 6}, {7}})
		checkStats(t, g, id, subscriber.Stats{Delivered: 7, Handled: 7, Batches: 3})

		g.Publish(t.Context(), 8)
		start := time.Now()
		if err := waitShutdown(t, shutdown(t.Context(), g)); err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Errorf("Shutdown waited %v for the window to end, want it released at once", waited)
		}
		checkBatches(t, batches, [][]int{{1, 2, 3}, {4, 5, 6}, {7}, {8}})
		if got := singles.messages(); len(got) != 0 {
			t.Errorf("handle got %v, want nothing", got)
		}
	})
}

// batchOf returns a batch of values.
func batchOf(values ...int) *message.Slice[int] {
	batch := message.Slice[int](values)

	return &batch
}

// batchRecorder is a batch handler recording the values of every batch, in order.
type batchRecorder struct {
	mu      sync.Mutex
	batches [][]int
}

func (r *batchRecorder) handle(_ context.Context, _ uint64, batch message.Batch[int]) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, slices.Collect(batch.All()))

	return nil
}

// checkBatches checks that r recorded want.
func checkBatches(t *testing.T, r *batchRecorder, want [][]int) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !slices.EqualFunc(r.batches, want, slices.Equal[[]int]) {
		t.Errorf("batch handler got %v, want %v", r.batches, want)
	}
}

// recordMiddleware returns a middleware recording each message into r.
func recordMiddleware(r *recorder[int]) subscriber.Middleware[int] {
	return func(next subscriber.Handler[int]) subscriber.Handler[int] {
		return func(ctx context.Context, id uint64, msg int) error {
			r.record(msg)

			return next(ctx, id, msg)
		}
	}
}

// describeBatchLoss describes an error about a whole batch, such as "dropped [1 2]", and fails on any other error, or a
// *DroppedError or *ClosedError whose text does not name the batch.
func describeBatchLoss(t *testing.T, err error) string {
	t.Helper()
	var (
		dropped *subscriber.DroppedError[int]
		timeout *subscriber.TimeoutError[int]
		closed  *subscriber.ClosedError[int]
		evicted *subscriber.EvictedError[int]
		batch   message.Batch[int]
		what    string
	)
	switch {
	case errors.As(err, &evicted):
		batch, what = evicted.Batch, "evicted by "+evicted.Err.Error()
	case errors.As(err, &dropped):
		batch, what = dropped.Batch, "dropped"
	case errors.As(err, &timeout):
		batch, what = timeout.Batch, "timed out"
	case errors.As(err, &closed):
		batch, what = closed.Batch, "closed"
	default:
		t.Errorf("error handler got %v, want an error about a batch", err)

		return err.Error()
	}
	if batch == nil {
		t.Errorf("%v holds no batch", err)

		return what
	}
	if named := strings.Contains(err.Error(), fmt.Sprintf("batch of %d messages", batch.Len())); !named && (dropped != nil || closed != nil) {
		t.Errorf("%q does not name the batch", err)
	}

	return fmt.Sprintf("%s %v", what, slices.Collect(batch.All()))
}

// tally is a batch of its own type.
type tally struct {
	values []int
}

func (b *tally) Add(msg int) {
	b.values = append(b.values, msg)
}

func (b *tally) All() iter.Seq[int] {
	return slices.Values(b.values)
}

func (b *tally) Len() int {
	return len(b.values)
}

func (b *tally) New() message.Batch[int] {
	return &tally{}
}
