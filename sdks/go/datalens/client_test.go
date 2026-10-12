package datalens

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

type reply struct {
	status  int
	body    string
	headers map[string]string
}

// fake is a datalens server with one chain, ethereum, and one log per block.
type fake struct {
	mu        sync.Mutex
	safe      uint64
	urls      []string
	bodies    []map[string]any
	headers   []http.Header
	script    []reply
	pendingAt map[uint64]bool
	onWait    func(*fake)
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.urls = append(f.urls, r.Method+" "+r.URL.RequestURI())
	f.bodies = append(f.bodies, body)
	f.headers = append(f.headers, r.Header.Clone())
	send := func(status int, value any) {
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(status)
		if text, ok := value.(string); ok {
			_, _ = w.Write([]byte(text))
			return
		}
		_ = json.NewEncoder(w).Encode(value)
	}
	if len(f.script) > 0 {
		next := f.script[0]
		f.script = f.script[1:]
		for name, value := range next.headers {
			w.Header().Set(name, value)
		}
		send(next.status, next.body)
		return
	}
	heads := ChainHeads{Chain: "ethereum", Latest: f.safe + 8, LatestHash: "0xaa", Safe: f.safe, Finalized: f.safe - 4}
	switch r.URL.Path {
	case "/v1/heads/ethereum":
		send(200, heads)
	case "/v1/heads/ethereum/wait":
		if f.onWait != nil {
			f.onWait(f)
		}
		heads.Safe = f.safe
		send(200, heads)
	case "/v1/watch":
		w.WriteHeader(http.StatusNoContent)
	case "/v1/query":
		send(200, f.query(body))
	default:
		send(404, `{"error":{"kind":"invalid_input","message":"no route"}}`)
	}
}

func (f *fake) query(body map[string]any) QueryResponse {
	blocks := body["range"].(map[string]any)
	start, end := uint64(blocks["start"].(float64)), uint64(blocks["end"].(float64))
	requested := BlockRange{Start: start, End: end}
	var reason IncompleteReason
	if f.pendingAt[start] {
		delete(f.pendingAt, start)
		end = start + 1
		reason = Pending
	}
	from, limit := start, uint64(10_000)
	if page, ok := body["page"].(map[string]any); ok {
		if value, ok := page["limit"].(float64); ok {
			limit = uint64(value)
		}
		if cursor, ok := page["cursor"].(string); ok {
			from, _ = strconv.ParseUint(cursor, 10, 64)
		}
	}
	to := min(end, from+limit-1)
	response := QueryResponse{
		Chain:            "ethereum",
		Dataset:          EvmLogs,
		Requested:        requested,
		Served:           &BlockRange{Start: start, End: end},
		IncompleteReason: reason,
		Segments:         []Segment{{Start: start, End: end, Source: Durable, Finality: Safe}},
		Rows:             []json.RawMessage{},
	}
	for block := from; block <= to; block++ {
		response.Rows = append(response.Rows, json.RawMessage(fmt.Sprintf(`{"block_number":%d,"log_index":0}`, block)))
	}
	if to < end {
		cursor := strconv.FormatUint(to+1, 10)
		response.Page.NextCursor = &cursor
	}
	if end < requested.End {
		next := end + 1
		response.NextStart = &next
	}
	return response
}

func serve(t *testing.T, f *fake) *Client {
	t.Helper()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	return NewClient(ClientConfig{
		Endpoint:    server.URL + "/",
		Application: "indexer",
		Token:       "secret",
		Retry:       RetryConfig{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond},
	})
}

func blocks(t *testing.T, response *QueryResponse) []uint64 {
	t.Helper()
	rows, err := response.LogRows()
	if err != nil {
		t.Fatal(err)
	}
	numbers := []uint64{}
	for _, row := range rows {
		numbers = append(numbers, row.BlockNumber)
	}
	return numbers
}

func equal(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

var selector = &LogSelector{Addresses: []string{"0xb1"}, Topics: [][]string{{"0x11"}, nil}}

func TestQueryAllMergesEveryPageAndSendsTheApplicationHeaders(t *testing.T) {
	f := &fake{safe: 9}
	client := serve(t, f)
	request := &QueryRequest{Chain: "ethereum", Dataset: EvmLogs, Selector: selector, Range: BlockRange{0, 6}, Page: &PageRequest{Limit: 3}}

	all, err := client.QueryAll(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, blocks(t, all), []uint64{0, 1, 2, 3, 4, 5, 6})
	equal(t, *all.Served, BlockRange{0, 6})
	equal(t, all.Page.NextCursor, (*string)(nil))
	equal(t, f.headers[0].Get("x-datalens-application"), "indexer")
	equal(t, f.headers[0].Get("authorization"), "Bearer secret")
	equal(t, f.bodies[0]["selector"], map[string]any{"addresses": []any{"0xb1"}, "topics": []any{[]any{"0x11"}, nil}})
	pages := []any{}
	for _, body := range f.bodies {
		pages = append(pages, body["page"])
	}
	equal(t, pages, []any{
		map[string]any{"limit": 3.0},
		map[string]any{"limit": 3.0, "cursor": "3"},
		map[string]any{"limit": 3.0, "cursor": "6"},
	})
}

func TestErrorsCarryKindAndStatusAndOnlyRetryableOnesAreRetried(t *testing.T) {
	f := &fake{safe: 9, script: []reply{{status: 403, body: `{"error":{"kind":"forbidden","message":"may not read base"}}`}}}
	client := serve(t, f)
	ctx := context.Background()

	_, err := client.Heads(ctx, "ethereum")
	var failure *Error
	if !errors.As(err, &failure) {
		t.Fatalf("%v", err)
	}
	equal(t, [3]any{failure.Kind, failure.Status, failure.Retryable()}, [3]any{"forbidden", 403, false})
	equal(t, err.Error(), "datalens 403 forbidden: may not read base")
	equal(t, len(f.urls), 1)

	f.script = []reply{
		{status: 503, body: "upstream down"},
		{status: 429, body: `{"error":{"kind":"quota_exceeded","message":"rpm"}}`, headers: map[string]string{"retry-after": "0"}},
	}
	heads, err := client.Heads(ctx, "ethereum")
	if err != nil {
		t.Fatal(err)
	}
	equal(t, heads.Safe, uint64(9))
	equal(t, len(f.urls), 4)

	f.script = []reply{{502, "<html>bad gateway</html>", nil}, {502, "<html>bad gateway</html>", nil}, {502, "<html>bad gateway</html>", nil}}
	_, err = client.Heads(ctx, "ethereum")
	errors.As(err, &failure)
	equal(t, [4]any{failure.Kind, failure.Status, failure.Message, failure.Retryable()}, [4]any{"http", 502, "<html>bad gateway</html>", true})
	equal(t, len(f.urls), 7)
}

func TestWaitHeadsSendsItsParametersAndUnwatchAcceptsNoContent(t *testing.T) {
	f := &fake{safe: 9}
	client := serve(t, f)
	heads, err := client.WaitHeads(context.Background(), "ethereum", 8, HeadSafe, 250*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, heads.Safe, uint64(9))
	if err := client.Unwatch(context.Background(), &WatchRequest{Chain: "ethereum", Dataset: EvmLogs, Selector: selector}); err != nil {
		t.Fatal(err)
	}
	equal(t, f.urls, []string{"GET /v1/heads/ethereum/wait?after=8&kind=safe&timeout_ms=250", "DELETE /v1/watch"})
}

func summary(event FollowEvent) string {
	switch event.Kind {
	case FollowBatch:
		return fmt.Sprintf("batch %d-%d rows=%d", event.Response.Served.Start, event.Response.Served.End, len(event.Response.Rows))
	case FollowCaughtUp:
		return fmt.Sprintf("caught_up %d", event.Head)
	case FollowStalled:
		return fmt.Sprintf("stalled %d %s", event.NextStart, event.Reason)
	case FollowSkipped:
		return fmt.Sprintf("skipped %d-%d", event.Blocks.Start, event.Blocks.End)
	default:
		var failure *Error
		errors.As(event.Err, &failure)
		return fmt.Sprintf("error %d retry=%t", failure.Status, event.RetryIn > 0)
	}
}

func collect(events <-chan FollowEvent) []string {
	out := []string{}
	for event := range events {
		out = append(out, summary(event))
	}
	return out
}

func TestFollowReadsInChunksWaitsOnHeadsAndResumesAtNextStart(t *testing.T) {
	f := &fake{safe: 9, pendingAt: map[uint64]bool{10: true}, onWait: func(f *fake) { f.safe = 14 }}
	client := serve(t, f)
	end := uint64(14)
	events := client.Follow(context.Background(), FollowRequest{
		Chain: "ethereum", Dataset: EvmLogs, Selector: selector,
		Start: 0, End: &end, ChunkBlocks: 5, PageLimit: 2, PollInterval: time.Millisecond,
	})
	equal(t, collect(events), []string{
		"batch 0-4 rows=5",
		"batch 5-9 rows=5",
		"caught_up 9",
		"batch 10-11 rows=2",
		"stalled 12 pending",
		"batch 12-14 rows=3",
	})
	waits := 0
	for _, url := range f.urls {
		if url == "GET /v1/heads/ethereum/wait?after=9&kind=safe&timeout_ms=30000" {
			waits++
		}
	}
	equal(t, waits, 1)
}

func TestFollowReportsRetryableFailuresAndStopsOnOthers(t *testing.T) {
	f := &fake{safe: 9, script: []reply{{503, "down", nil}, {503, "down", nil}, {503, "down", nil}}}
	client := serve(t, f)
	end := uint64(3)
	request := FollowRequest{Chain: "ethereum", Dataset: EvmLogs, Selector: selector, Start: 0, End: &end}
	equal(t, collect(client.Follow(context.Background(), request)), []string{"error 503 retry=true", "batch 0-3 rows=4"})

	f.script = []reply{{401, `{"error":{"kind":"unauthorized","message":"bad token"}}`, nil}}
	equal(t, collect(client.Follow(context.Background(), request)), []string{"error 401 retry=false"})
}

func TestQueryAllRereadsWhenALaterPageDisagreesOnABlockHash(t *testing.T) {
	page := func(blockHash, cursor string) string {
		next := "null"
		if cursor != "" {
			next = strconv.Quote(cursor)
		}
		return fmt.Sprintf(`{"chain":"ethereum","dataset":"evm.logs","requested":{"start":10,"end":11},"served":{"start":10,"end":11},"next_start":null,"incomplete_reason":"","segments":[],"rows":[{"block_number":10,"log_index":0,"block_hash":"%s"}],"page":{"next_cursor":%s}}`, blockHash, next)
	}
	f := &fake{script: []reply{
		{200, page("0x11", "c"), nil},
		{200, page("0x22", ""), nil},
		{200, page("0x11", "c"), nil},
		{200, page("0x11", ""), nil},
	}}
	client := serve(t, f)
	merged, err := client.QueryAll(context.Background(), &QueryRequest{
		Chain: "ethereum", Dataset: EvmLogs, Range: BlockRange{Start: 10, End: 11},
	})
	if err != nil {
		t.Fatal(err)
	}
	equal(t, len(merged.Rows), 2)
	equal(t, len(f.urls), 4)
}

func TestLatestOnlyFollowNamesBlocksLeftBelowAMovedSafeHead(t *testing.T) {
	heads := `{"chain":"ethereum","latest":17,"latest_hash":"0xaa","safe":9,"finalized":4}`
	query := `{"chain":"ethereum","dataset":"evm.logs","requested":{"start":10,"end":14},"served":{"start":12,"end":14},"next_start":null,"incomplete_reason":"below_safe","segments":[],"rows":[],"page":{"next_cursor":null}}`
	f := &fake{script: []reply{{200, heads, nil}, {200, query, nil}}}
	client := serve(t, f)
	end := uint64(14)
	events := collect(client.Follow(context.Background(), FollowRequest{
		Chain: "ethereum", Dataset: EvmLogs, Finality: LatestOnly, Start: 10, End: &end,
	}))
	equal(t, events, []string{"skipped 10-11", "batch 12-14 rows=0"})
	equal(t, len(f.urls), 2)
}

func TestBlockRecordKeepsUnknownHeaderFields(t *testing.T) {
	raw := []byte(`{"number":1,"hash":"0x1","parent_hash":"0x0","timestamp":9,"finality":"safe","gasUsed":"0x10"}`)
	var block BlockRecord
	if err := json.Unmarshal(raw, &block); err != nil {
		t.Fatal(err)
	}
	equal(t, block.Number, uint64(1))
	equal(t, string(block.Extra["gasUsed"]), `"0x10"`)
}

func TestFollowStopsWhenTheContextIsCancelled(t *testing.T) {
	f := &fake{safe: 9}
	client := serve(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	events := client.Follow(ctx, FollowRequest{Chain: "ethereum", Dataset: EvmLogs, Selector: selector, Start: 10, HeadsWait: time.Millisecond})
	equal(t, summary(<-events), "caught_up 9")
	cancel()
	for range events {
	}
}
