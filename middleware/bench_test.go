package middleware_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/elojah/multicast/middleware"
)

// BenchmarkFirstN measures one message under FirstN: without a race, too late for one, and won alone or against losers
// still running, which the win cancels. Those last two count creating the race.
func BenchmarkFirstN(b *testing.B) {
	first := middleware.FirstN(1, raceOf)
	win := first(func(context.Context, uint64, job) error { return nil })

	b.Run("no race", func(b *testing.B) {
		ctx := b.Context()
		for b.Loop() {
			_ = win(ctx, 1, job{})
		}
	})
	b.Run("skip", func(b *testing.B) {
		ctx := b.Context()
		msg := job{race: middleware.NewRace()}
		msg.race.Stop()
		for b.Loop() {
			_ = win(ctx, 1, msg)
		}
	})
	b.Run("win", func(b *testing.B) {
		ctx := b.Context()
		for b.Loop() {
			_ = win(ctx, 1, job{race: middleware.NewRace()})
		}
	})
	for _, losers := range []uint64{1, 7} {
		b.Run(fmt.Sprintf("losers=%d", losers), func(b *testing.B) {
			ctx := b.Context()
			running := make(chan struct{})
			lose := first(func(ctx context.Context, _ uint64, _ job) error {
				running <- struct{}{}
				<-ctx.Done()

				return ctx.Err()
			})
			for b.Loop() {
				msg := job{race: middleware.NewRace()}
				var wg sync.WaitGroup
				for id := range losers {
					wg.Go(func() { _ = lose(ctx, id, msg) })
				}
				for range losers {
					<-running
				}
				_ = win(ctx, losers, msg)
				wg.Wait()
			}
		})
	}
}
