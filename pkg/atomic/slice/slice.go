// Package slice provides Slice, a slice read without a lock and copied on every write.
package slice

import (
	"slices"
	"sync"
	"sync/atomic"
)

// Slice holds elements that Load reads without a lock, while every write copies them under a mutex, so a slice Load
// returned never changes. The zero value is empty. It must not be copied after first use.
type Slice[E any] struct {
	mu    sync.Mutex // held by every write
	elems atomic.Pointer[[]E]
}

// Load returns the elements, which must not be changed.
func (s *Slice[E]) Load() []E {
	if elems := s.elems.Load(); elems != nil {
		return *elems
	}

	return nil
}

// Insert adds e where cmp sorts it, so the elements stay sorted if every Insert uses the same cmp.
func (s *Slice[E]) Insert(e E, cmp func(a, b E) int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.Load()
	i, _ := slices.BinarySearchFunc(old, e, cmp)
	elems := slices.Concat(old[:i], []E{e}, old[i:])
	s.elems.Store(&elems)
}

// Delete removes the first element match accepts and returns it, or returns false if none does.
func (s *Slice[E]) Delete(match func(e E) bool) (E, bool) { //nolint:ireturn // E is the caller's type
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.Load()
	i := slices.IndexFunc(old, match)
	if i < 0 {
		var zero E

		return zero, false
	}
	elems := slices.Concat(old[:i], old[i+1:])
	s.elems.Store(&elems)

	return old[i], true
}

// Clear removes every element and returns them.
func (s *Slice[E]) Clear() []E {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.Load()
	s.elems.Store(nil)

	return old
}
