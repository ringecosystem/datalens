// Package datalens is a client for the datalens query API. Field names on the wire are
// the server's, in snake_case.
package datalens

import "encoding/json"

type Dataset string

const (
	EvmLogs   Dataset = "evm.logs"
	EvmBlocks Dataset = "evm.blocks"
)

// QueryFinality is which part of the chain a query may read.
type QueryFinality string

const (
	DurableOnly  QueryFinality = "durable_only"
	SafeToLatest QueryFinality = "safe_to_latest"
	LatestOnly   QueryFinality = "latest_only"
)

type Finality string

const (
	Unsafe    Finality = "unsafe"
	Safe      Finality = "safe"
	Finalized Finality = "finalized"
)

// IncompleteReason is empty when the whole request was served.
type IncompleteReason string

const (
	Pending         IncompleteReason = "pending"
	ProviderError   IncompleteReason = "provider_error"
	BudgetExhausted IncompleteReason = "budget_exhausted"
	NotFollowedYet  IncompleteReason = "not_followed_yet"
)

type SegmentSource string

const (
	Durable SegmentSource = "durable"
	Hot     SegmentSource = "hot"
)

type HeadKind string

const (
	HeadLatest    HeadKind = "latest"
	HeadSafe      HeadKind = "safe"
	HeadFinalized HeadKind = "finalized"
)

// BlockRange is inclusive.
type BlockRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

// LogSelector filters evm.logs. A nil topic position matches any topic.
type LogSelector struct {
	Addresses []string   `json:"addresses,omitempty"`
	Topics    [][]string `json:"topics,omitempty"`
}

type PageRequest struct {
	Limit  uint32 `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

type QueryRequest struct {
	Chain    string        `json:"chain"`
	Dataset  Dataset       `json:"dataset"`
	Selector *LogSelector  `json:"selector,omitempty"`
	Range    BlockRange    `json:"range"`
	Finality QueryFinality `json:"finality,omitempty"`
	// Fields omitted means every base field. block_timestamp and transaction_from only when listed.
	Fields []string     `json:"fields,omitempty"`
	Page   *PageRequest `json:"page,omitempty"`
	WaitMs *uint64      `json:"wait_ms,omitempty"`
}

type Anchor struct {
	Number uint64 `json:"number"`
	Hash   string `json:"hash"`
}

type Segment struct {
	Start    uint64        `json:"start"`
	End      uint64        `json:"end"`
	Source   SegmentSource `json:"source"`
	Finality Finality      `json:"finality"`
	// Anchor is set on hot segments: the follow head, possibly above End.
	Anchor *Anchor `json:"anchor,omitempty"`
}

type LogRecord struct {
	BlockNumber      uint64   `json:"block_number"`
	BlockHash        string   `json:"block_hash"`
	BlockTimestamp   *uint64  `json:"block_timestamp,omitempty"`
	TransactionHash  string   `json:"transaction_hash"`
	TransactionIndex uint32   `json:"transaction_index"`
	LogIndex         uint32   `json:"log_index"`
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	Finality         Finality `json:"finality"`
	TransactionFrom  string   `json:"transaction_from,omitempty"`
}

// BlockRecord holds the named header fields. Decode a row into a map for the others,
// which keep their JSON-RPC names such as gasUsed.
type BlockRecord struct {
	Number     uint64   `json:"number"`
	Hash       string   `json:"hash"`
	ParentHash string   `json:"parent_hash"`
	Timestamp  uint64   `json:"timestamp"`
	Finality   Finality `json:"finality"`
}

type PageInfo struct {
	NextCursor *string `json:"next_cursor"`
}

type QueryResponse struct {
	Chain     string     `json:"chain"`
	Dataset   Dataset    `json:"dataset"`
	Requested BlockRange `json:"requested"`
	// Served is a contiguous prefix of Requested, nil when nothing is served.
	Served *BlockRange `json:"served"`
	// NextStart is the first block not served, nil when the whole request is served.
	NextStart        *uint64           `json:"next_start"`
	IncompleteReason IncompleteReason  `json:"incomplete_reason"`
	IncompleteDetail string            `json:"incomplete_detail"`
	Segments         []Segment         `json:"segments"`
	Rows             []json.RawMessage `json:"rows"`
	Page             PageInfo          `json:"page"`
}

// LogRows decodes Rows of an evm.logs response.
func (r *QueryResponse) LogRows() ([]LogRecord, error) {
	return decodeRows[LogRecord](r.Rows)
}

// BlockRows decodes Rows of an evm.blocks response.
func (r *QueryResponse) BlockRows() ([]BlockRecord, error) {
	return decodeRows[BlockRecord](r.Rows)
}

func decodeRows[T any](rows []json.RawMessage) ([]T, error) {
	out := make([]T, len(rows))
	for i, row := range rows {
		if err := json.Unmarshal(row, &out[i]); err != nil {
			return nil, &Error{Type: DecodeError, Message: err.Error(), Err: err}
		}
	}
	return out, nil
}

type ChainHeads struct {
	Chain      string `json:"chain"`
	Latest     uint64 `json:"latest"`
	LatestHash string `json:"latest_hash"`
	Safe       uint64 `json:"safe"`
	Finalized  uint64 `json:"finalized"`
}

// Of returns the head of the given kind.
func (h ChainHeads) Of(kind HeadKind) uint64 {
	switch kind {
	case HeadSafe:
		return h.Safe
	case HeadFinalized:
		return h.Finalized
	default:
		return h.Latest
	}
}

type WatchRequest struct {
	Chain    string       `json:"chain"`
	Dataset  Dataset      `json:"dataset"`
	Selector *LogSelector `json:"selector,omitempty"`
}

type FollowInfo struct {
	FromBlock  uint64  `json:"from_block"`
	HeadBlock  *uint64 `json:"head_block"`
	HeadHash   *string `json:"head_hash"`
	PromotedTo *uint64 `json:"promoted_to"`
}

type WatchResponse struct {
	Chain      string     `json:"chain"`
	Dataset    Dataset    `json:"dataset"`
	SelectorID int64      `json:"selector_id"`
	TTLMs      uint64     `json:"ttl_ms"`
	Follow     FollowInfo `json:"follow"`
}
