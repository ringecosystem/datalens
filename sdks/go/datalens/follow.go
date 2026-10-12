package datalens

import (
	"context"
	"errors"
	"time"
)

type FollowRequest struct {
	Chain    string
	Dataset  Dataset
	Selector *LogSelector
	// Finality DurableOnly (the default) follows the safe head. The other two follow latest.
	Finality QueryFinality
	Fields   []string
	Start    uint64
	// End is the last block to read. Nil follows the head until ctx is done.
	End *uint64
	// ChunkBlocks is the most blocks per query, at or below the application's
	// max_range_blocks. Zero means 1000.
	ChunkBlocks uint64
	PageLimit   uint32
	WaitMs      *uint64
	// HeadsWait is the heads/wait long-poll timeout once caught up. Zero means 30 s.
	HeadsWait time.Duration
	// PollInterval is the pause after pending or not_followed_yet. Zero means 1 s.
	PollInterval time.Duration
}

type FollowEventKind string

const (
	// FollowBatch carries one query with every page merged. Its rows cover Served, a prefix
	// of Requested.
	FollowBatch FollowEventKind = "batch"
	// FollowCaughtUp means everything up to Head has been read. Follow long-polls until the
	// head passes it.
	FollowCaughtUp FollowEventKind = "caught_up"
	// FollowSkipped means latest_only left Blocks at or below the safe head.
	FollowSkipped FollowEventKind = "skipped"
	// FollowStalled means the server stopped at NextStart. Follow asks again after RetryIn.
	FollowStalled FollowEventKind = "stalled"
	// FollowError carries Err. Follow retries after RetryIn when Err is retryable, and
	// otherwise closes the channel after this event.
	FollowError FollowEventKind = "error"
)

type FollowEvent struct {
	Kind      FollowEventKind
	Response  *QueryResponse
	Blocks    BlockRange
	Head      uint64
	NextStart uint64
	Reason    IncompleteReason
	Detail    string
	RetryIn   time.Duration
	Err       error
}

// Follow reads request.Start on chunk by chunk, then follows the head. The channel closes
// when End is passed, ctx is done, or a failure is not retryable.
func (c *Client) Follow(ctx context.Context, request FollowRequest) <-chan FollowEvent {
	events := make(chan FollowEvent)
	go func() {
		defer close(events)
		follower{client: c, request: request, events: events}.run(ctx)
	}()
	return events
}

type follower struct {
	client  *Client
	request FollowRequest
	events  chan<- FollowEvent
}

func (f follower) run(ctx context.Context) {
	r := f.request
	finality := r.Finality
	if finality == "" {
		finality = DurableOnly
	}
	kind := HeadLatest
	if finality == DurableOnly {
		kind = HeadSafe
	}
	chunk := r.ChunkBlocks
	if chunk == 0 {
		chunk = 1000
	}
	headsWait := r.HeadsWait
	if headsWait == 0 {
		headsWait = 30 * time.Second
	}
	poll := r.PollInterval
	if poll == 0 {
		poll = time.Second
	}
	next := r.Start
	var heads *ChainHeads
	caughtUp := false
	failures := 0

	emit := func(event FollowEvent) bool {
		select {
		case f.events <- event:
			return true
		case <-ctx.Done():
			return false
		}
	}
	pause := func(delay time.Duration) bool { return sleep(ctx, delay) == nil }
	fail := func(err error) bool {
		var failure *Error
		if ctx.Err() != nil || !errors.As(err, &failure) || !failure.Retryable() {
			emit(FollowEvent{Kind: FollowError, Err: err})
			return false
		}
		failures++
		delay := failure.RetryAfter
		if delay == 0 {
			delay = f.client.retry.Backoff(failures)
		}
		return emit(FollowEvent{Kind: FollowError, Err: err, RetryIn: delay}) && pause(delay)
	}
	skip := func(last uint64) bool {
		if last < next {
			return true
		}
		blocks := BlockRange{Start: next, End: last}
		next = last + 1
		return emit(FollowEvent{Kind: FollowSkipped, Blocks: blocks})
	}

	for r.End == nil || next <= *r.End {
		current := heads
		heads = nil
		if current == nil {
			fetched, err := f.client.Heads(ctx, r.Chain)
			if err != nil {
				if !fail(err) {
					return
				}
				continue
			}
			current = fetched
		}
		if finality == LatestOnly && next <= current.Safe {
			last := current.Safe
			if r.End != nil {
				last = min(last, *r.End)
			}
			if !skip(last) {
				return
			}
			heads = current
			continue
		}
		tip := current.Of(kind)
		last := tip
		if r.End != nil {
			last = min(last, *r.End)
		}
		if next > last {
			if !caughtUp {
				caughtUp = true
				heads = current
				if !emit(FollowEvent{Kind: FollowCaughtUp, Head: tip}) {
					return
				}
				continue
			}
			waited, err := f.client.WaitHeads(ctx, r.Chain, next-1, kind, headsWait)
			if err != nil {
				if !fail(err) {
					return
				}
				continue
			}
			heads = waited
			continue
		}
		caughtUp = false

		query := QueryRequest{
			Chain:    r.Chain,
			Dataset:  r.Dataset,
			Selector: r.Selector,
			Range:    BlockRange{Start: next, End: min(last, next+chunk-1)},
			Finality: finality,
			Fields:   r.Fields,
			WaitMs:   r.WaitMs,
		}
		if r.PageLimit != 0 {
			query.Page = &PageRequest{Limit: r.PageLimit}
		}
		response, err := f.client.QueryAll(ctx, &query)
		if err != nil {
			if !fail(err) {
				return
			}
			continue
		}
		resume := query.Range.End + 1
		if response.NextStart != nil {
			resume = *response.NextStart
		}
		if resume > next {
			failures = 0
		}
		var unserved *uint64
		if response.Served != nil {
			if response.Served.Start > 0 {
				last := response.Served.Start - 1
				unserved = &last
			}
		} else if resume > 0 {
			last := resume - 1
			unserved = &last
		}
		if unserved != nil && !skip(*unserved) {
			return
		}
		next = resume
		if response.Served != nil && !emit(FollowEvent{Kind: FollowBatch, Response: response}) {
			return
		}
		if response.IncompleteReason != "" && response.IncompleteReason != BelowSafe {
			delay := poll
			if response.IncompleteReason == ProviderError || response.IncompleteReason == BudgetExhausted {
				failures++
				delay = f.client.retry.Backoff(failures)
			}
			stalled := FollowEvent{
				Kind:      FollowStalled,
				NextStart: resume,
				Reason:    response.IncompleteReason,
				Detail:    response.IncompleteDetail,
				RetryIn:   delay,
			}
			if !emit(stalled) || !pause(delay) {
				return
			}
		}
	}
}
