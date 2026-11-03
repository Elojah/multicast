// Package holder holds ready-made subscriber.Holders for reply's requests:
//
//	id, err := g.Subscribe(ctx, handle, subscriber.WithHolder(holder.NewLatest[string, float64](time.Second)))
//
// NewLatest answers the requests it drops, which package holder's leaves waiting for ctx. NewReorder and NewJitter sort
// requests by Query. Package holder's NewBatch, NewReorderFunc and NewJitterFunc take requests as they are, and so does
// NewReduce, if it merges what it drops with reply.Request.Merge.
package holder
