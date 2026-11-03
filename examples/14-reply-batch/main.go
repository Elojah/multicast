// A reply.Group's subscriber can answer several requests at once. Here the gateway's sensors sit on one bus, which
// reads several of them in one request: the bus gathers the queries that come together into batches of three
// (holder.NewBatch), and its batch handler (subscriber.WithBatchHandler) reads each batch in one request, then answers
// every query of it with Request.Answer.
//
// Once the bus is down, the batch handler returns one error, which answers every query of the batch at once, rather
// than leaving each Publish to wait for its ctx. reply.Group's Subscribe makes it so: with subscriber.WithBatchMiddleware,
// it wraps the batch handler in a middleware turning its error into a *subscriber.HandleError about the batch, which
// answers each query of it. A batch middleware wraps the batch handler whichever option sets it, so the logging one
// here, from multicast.WithDefaultSubscriberOptions, wraps the one Subscribe sets. Default options come first, so it
// runs outside reply's, and logs the error once reply has wrapped it.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elojah/multicast"
	"github.com/elojah/multicast/holder"
	"github.com/elojah/multicast/message"
	"github.com/elojah/multicast/reply"
	"github.com/elojah/multicast/subscriber"
)

var errBusDown = errors.New("bus down")

// query asks the bus for a sensor's reading.
type query = reply.Request[string, float64]

// batch is what the bus's batch handler gets.
type batch = message.Batch[query]

func main() {
	ctx := context.Background()
	g := reply.New(multicast.WithDefaultSubscriberOptions(func() []subscriber.Option[query] {
		return []subscriber.Option[query]{subscriber.WithBatchMiddleware(logErrors)}
	}))

	registers := map[string]float64{"north": 21.5, "south": 23, "east": 19}
	var down atomic.Bool
	read := func(_ context.Context, id uint64, b batch) error {
		sensors := make([]string, 0, b.Len())
		for q := range b.All() {
			sensors = append(sensors, q.Query)
		}
		// Sorted, since the queries come in any order.
		slices.Sort(sensors)
		fmt.Println("bus: one request for", strings.Join(sensors, ", "))
		if down.Load() {
			return errBusDown
		}
		for q := range b.All() {
			q.Answer(id, registers[q.Query], nil)
		}

		return nil
	}
	// The holder releases every query in a batch, to read, so handle gets none.
	if _, err := g.Subscribe(ctx, func(_ context.Context, _ uint64, sensor string) (float64, error) {
		return registers[sensor], nil
	}, subscriber.WithHolder(holder.NewBatch[query](time.Hour, 3)), subscriber.WithBatchHandler(read)); err != nil {
		log.Fatal(err)
	}

	ask(ctx, g, "north", "south", "east")
	down.Store(true)
	ask(ctx, g, "north", "south", "east")

	if err := g.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}

// ask asks for each sensor's reading at once, as a dashboard, an alarm rule and a logger would, each Publish waiting
// for its answer, then prints the answers.
func ask(ctx context.Context, g *reply.Group[string, float64], sensors ...string) {
	// A deadline, for a bus that would never answer.
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	answers := make([]reply.Result[float64], len(sensors))
	var wg sync.WaitGroup
	for i, sensor := range sensors {
		wg.Go(func() {
			// One subscriber, so one answer.
			for _, answer := range g.Publish(ctx, sensor) {
				answers[i] = answer
			}
		})
	}
	wg.Wait()

	for i, sensor := range sensors {
		if err := answers[i].Err; err != nil {
			fmt.Printf("%s: %v\n", sensor, err)

			continue
		}
		fmt.Printf("%s: %.1f°C\n", sensor, answers[i].Value)
	}
}

// logErrors logs the error of the batch handler it wraps. It prints before the error is reported, so before it answers
// any query.
func logErrors(next subscriber.Handler[batch]) subscriber.Handler[batch] {
	return func(ctx context.Context, id uint64, b batch) error {
		err := next(ctx, id, b)
		if err != nil {
			fmt.Println("log:", err)
		}

		return err
	}
}
