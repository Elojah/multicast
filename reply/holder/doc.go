// Package holder holds ready-made subscriber.Holders for reply's requests:
//
//	id, err := g.Subscribe(ctx, handle, subscriber.WithHolder(holder.NewLatest[string, float64](time.Second)))
//
// NewLatest answers the requests it merges away with reply.Request.Merge, so that their Publish need not wait for ctx,
// which package holder's NewLatest leaves them to. NewReorder and NewJitter sort requests by Query. Package holder's
// other holders take requests as they are: NewBatch, and NewReorderFunc and NewJitterFunc comparing Queries, but
// NewReduce must merge what it drops with Merge.
package holder
