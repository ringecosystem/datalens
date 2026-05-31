package datalens

import "encoding/json"

type EventFilter struct {
	IndexName string
	Chain     string
	ChainID   *int
	Dataset   string
	Address   string
	EventName string
	Signature string
	FromBlock *int
	ToBlock   *int
	Topic0    string
	Limit     int
	First     int
	After     string
}

func (f EventFilter) variables(connection bool) map[string]any {
	variables := map[string]any{
		"dataset": f.Dataset,
	}
	setString(variables, "indexName", f.IndexName)
	setString(variables, "chain", f.Chain)
	setString(variables, "address", f.Address)
	setString(variables, "eventName", f.EventName)
	setString(variables, "signature", f.Signature)
	setString(variables, "topic0", f.Topic0)
	setIntPtr(variables, "chainId", f.ChainID)
	setIntPtr(variables, "fromBlock", f.FromBlock)
	setIntPtr(variables, "toBlock", f.ToBlock)
	if connection {
		if f.First > 0 {
			variables["first"] = f.First
		}
	} else if f.Limit > 0 {
		variables["limit"] = f.Limit
	}
	if f.After != "" {
		variables["after"] = f.After
	}
	return variables
}

type PageInfo struct {
	EndCursor   string `json:"endCursor"`
	HasNextPage bool   `json:"hasNextPage"`
}

type IndexedEventConnection struct {
	Edges    []IndexedEventEdge `json:"edges"`
	Nodes    []IndexedEvent     `json:"nodes"`
	PageInfo PageInfo           `json:"pageInfo"`
}

type IndexedEventEdge struct {
	Cursor string       `json:"cursor"`
	Node   IndexedEvent `json:"node"`
}

type DecodedEventConnection struct {
	Edges    []DecodedEventEdge `json:"edges"`
	Nodes    []DecodedEvent     `json:"nodes"`
	PageInfo PageInfo           `json:"pageInfo"`
}

type DecodedEventEdge struct {
	Cursor string       `json:"cursor"`
	Node   DecodedEvent `json:"node"`
}

type IndexedEvent struct {
	IndexName        string          `json:"indexName,omitempty"`
	Chain            string          `json:"chain,omitempty"`
	ChainID          int             `json:"chainId,omitempty"`
	Dataset          string          `json:"dataset,omitempty"`
	BlockNumber      int             `json:"blockNumber,omitempty"`
	BlockHash        string          `json:"blockHash,omitempty"`
	TransactionHash  string          `json:"transactionHash,omitempty"`
	TransactionIndex int             `json:"transactionIndex,omitempty"`
	EventIndex       int             `json:"eventIndex,omitempty"`
	Address          string          `json:"address,omitempty"`
	Selector         string          `json:"selector,omitempty"`
	Topics           []string        `json:"topics,omitempty"`
	Topic0           string          `json:"topic0,omitempty"`
	Signature        string          `json:"signature,omitempty"`
	EventName        string          `json:"eventName,omitempty"`
	Decoded          json.RawMessage `json:"decoded,omitempty"`
	Data             string          `json:"data,omitempty"`
	Payload          json.RawMessage `json:"payload,omitempty"`
	CreatedAt        string          `json:"createdAt,omitempty"`
}

type DecodedEvent struct {
	IndexName        string          `json:"indexName,omitempty"`
	Chain            string          `json:"chain,omitempty"`
	ChainID          int             `json:"chainId,omitempty"`
	Dataset          string          `json:"dataset,omitempty"`
	BlockNumber      int             `json:"blockNumber,omitempty"`
	BlockHash        string          `json:"blockHash,omitempty"`
	TransactionHash  string          `json:"transactionHash,omitempty"`
	TransactionIndex int             `json:"transactionIndex,omitempty"`
	LogIndex         int             `json:"logIndex,omitempty"`
	Address          string          `json:"address,omitempty"`
	EventName        string          `json:"eventName,omitempty"`
	Signature        string          `json:"signature,omitempty"`
	Topic0           string          `json:"topic0,omitempty"`
	DecodedArgs      json.RawMessage `json:"decodedArgs,omitempty"`
	DecodeStatus     string          `json:"decodeStatus,omitempty"`
	DecodeError      string          `json:"decodeError,omitempty"`
	Payload          json.RawMessage `json:"payload,omitempty"`
	CreatedAt        string          `json:"createdAt,omitempty"`
}

type Discovery struct {
	Chains []ChainDiscovery `json:"chains"`
}

type ChainDiscovery struct {
	Identity json.RawMessage    `json:"identity"`
	Datasets []DatasetDiscovery `json:"datasets"`
}

type DatasetDiscovery struct {
	DatasetKey string          `json:"datasetKey"`
	RangeKinds json.RawMessage `json:"rangeKinds"`
	Selectors  []string        `json:"selectors"`
	Enabled    bool            `json:"enabled"`
}

type QueryInput struct {
	Chain      ChainIdentity  `json:"chain"`
	DatasetKey DatasetKey     `json:"datasetKey"`
	Selector   QuerySelector  `json:"selector"`
	Range      QueryRange     `json:"range"`
	Finality   string         `json:"finality,omitempty"`
	Fields     FieldSelection `json:"fields,omitempty"`
}

type ChainIdentity struct {
	Family         ChainFamily `json:"family"`
	ConfiguredName string      `json:"configuredName"`
	NetworkID      *NetworkID  `json:"networkId,omitempty"`
}

type ChainFamily struct {
	Kind  string `json:"kind"`
	Other string `json:"other,omitempty"`
}

type NetworkID struct {
	Numeric *int   `json:"numeric,omitempty"`
	Textual string `json:"textual,omitempty"`
}

type DatasetKey struct {
	Family string `json:"family"`
	Name   string `json:"name"`
}

type QuerySelector struct {
	Kind    string           `json:"kind"`
	EVMLogs *EVMLogsSelector `json:"evmLogs,omitempty"`
	Other   *OtherSelector   `json:"other,omitempty"`
}

type EVMLogsSelector struct {
	Addresses []string   `json:"addresses,omitempty"`
	Topics    [][]string `json:"topics,omitempty"`
}

type OtherSelector struct {
	Kind         string `json:"kind"`
	Fingerprint  string `json:"fingerprint"`
	CanonicalKey string `json:"canonicalKey"`
}

type QueryRange struct {
	Kind  string `json:"kind"`
	Start int    `json:"start"`
	End   int    `json:"end"`
}

type FieldSelection struct {
	Include []string `json:"include,omitempty"`
}

type QueryResponse struct {
	Chain      json.RawMessage `json:"chain"`
	DatasetKey string          `json:"datasetKey"`
	Range      json.RawMessage `json:"range"`
	Cache      json.RawMessage `json:"cache"`
	Rows       json.RawMessage `json:"rows"`
}

func setString(values map[string]any, key string, value string) {
	if value != "" {
		values[key] = value
	}
}

func setIntPtr(values map[string]any, key string, value *int) {
	if value != nil {
		values[key] = *value
	}
}
