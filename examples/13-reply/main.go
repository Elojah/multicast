// A reply.Group's Subscribe takes a handle that returns an answer. Its Publish sends a query, then waits for an answer
// from each subscriber it reached, or until its ctx is done. Here a gateway asks its sensors for their temperature:
// the attic's is offline and answers with its error, and the cellar's measures humidity, so its filter keeps the query
// out.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"time"

	"github.com/elojah/multicast/reply"
	"github.com/elojah/multicast/subscriber"
)

var errOffline = errors.New("offline")

// query asks the sensors measuring a quantity for their reading.
type query = reply.Request[string, float64]

func main() {
	ctx := context.Background()
	g := reply.New[string, float64]()

	names := map[uint64]string{}
	for _, sensor := range []struct {
		name, measures string
		reading        float64
		err            error
	}{
		{name: "north", measures: "temperature", reading: 21.5},
		{name: "south", measures: "temperature", reading: 23},
		{name: "attic", measures: "temperature", err: errOffline},
		{name: "cellar", measures: "humidity", reading: 71},
	} {
		id, err := g.Subscribe(ctx, func(context.Context, uint64, string) (float64, error) {
			return sensor.reading, sensor.err
		}, subscriber.WithFilter(func(q query) bool { return q.Query == sensor.measures }))
		if err != nil {
			log.Fatal(err)
		}
		names[id] = sensor.name
	}

	// A deadline, for a sensor that would never answer.
	askCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	results := g.Publish(askCtx, "temperature")
	fmt.Println(len(results), "sensors asked")

	// The answers come by ID: printed by name.
	for _, id := range slices.SortedFunc(maps.Keys(results), func(x, y uint64) int { return cmp.Compare(names[x], names[y]) }) {
		if err := results[id].Err; err != nil {
			fmt.Printf("%s: %v\n", names[id], err)

			continue
		}
		fmt.Printf("%s: %.1f°C\n", names[id], results[id].Value)
	}

	if err := g.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
