package slice_test

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/elojah/multicast/pkg/atomic/slice"
)

// A slice Load returned keeps its elements whatever writes come after.
func TestSlice(t *testing.T) {
	t.Parallel()

	var s slice.Slice[int]
	for _, e := range []int{3, 1, 4, 2} {
		s.Insert(e, cmp.Compare[int])
	}
	loaded := s.Load()
	if want := []int{1, 2, 3, 4}; !slices.Equal(loaded, want) {
		t.Fatalf("Load() = %v after Insert, want %v", loaded, want)
	}

	if e, ok := s.Delete(func(e int) bool { return e%2 == 0 }); !ok || e != 2 {
		t.Errorf("Delete(even) = %d, %t, want 2, true", e, ok)
	}
	if _, ok := s.Delete(func(e int) bool { return e > 4 }); ok {
		t.Error("Delete(> 4) found an element")
	}
	if got, want := s.Load(), []int{1, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("Load() = %v after Delete, want %v", got, want)
	}
	if got, want := s.Clear(), []int{1, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("Clear() = %v, want %v", got, want)
	}
	if got := s.Load(); got != nil {
		t.Errorf("Load() = %v after Clear, want nil", got)
	}

	if want := []int{1, 2, 3, 4}; !slices.Equal(loaded, want) {
		t.Errorf("a slice loaded before the writes became %v, want %v", loaded, want)
	}
}

// Racing writes are each applied once, and racing Deletes of one element find it once.
func TestSlice_Concurrent(t *testing.T) {
	t.Parallel()

	const writers = 10
	var (
		s       slice.Slice[int]
		deleted atomic.Int64
		wg      sync.WaitGroup
	)
	for e := range writers {
		wg.Go(func() { s.Insert(e, cmp.Compare[int]) })
		wg.Go(func() {
			if loaded := s.Load(); !slices.IsSorted(loaded) {
				t.Errorf("Load() = %v during racing Insert, want it sorted", loaded)
			}
		})
	}
	wg.Wait()
	if got := s.Load(); len(got) != writers || !slices.IsSorted(got) {
		t.Fatalf("Load() = %v after %d racing Insert, want 0 to %d sorted", got, writers, writers-1)
	}

	for range writers {
		wg.Go(func() {
			if _, ok := s.Delete(func(e int) bool { return e == 0 }); ok {
				deleted.Add(1)
			}
		})
	}
	wg.Wait()
	if got := deleted.Load(); got != 1 {
		t.Errorf("racing Delete of one element found it %d times, want once", got)
	}
}
