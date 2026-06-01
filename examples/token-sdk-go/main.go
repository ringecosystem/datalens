package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/helixbox/datalens/sdks/go/datalens"
)

const (
	ethereumUSDCAddress    = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
	solanaUSDCMint         = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	tronUSDTContract       = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
	tronUSDTContractHex    = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	erc20TransferTopic0    = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	erc20TransferSignature = "Transfer(address,address,uint256)"
)

type rangeConfig struct {
	Start int
	End   int
	First int
}

type runtimeConfig struct {
	Endpoint    string
	Token       string
	Application string
	Ethereum    rangeConfig
	Solana      rangeConfig
	Tron        rangeConfig
}

type exampleQueries struct {
	Ethereum datalens.EventFilter
	Solana   datalens.QueryInput
	Tron     datalens.QueryInput
}

type tokenClient interface {
	DecodedEventsConnection(context.Context, datalens.EventFilter) (datalens.DecodedEventConnection, error)
	QueryNative(context.Context, datalens.QueryInput) (datalens.QueryResponse, error)
}

func main() {
	config, err := buildRuntimeConfig(osEnv())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	client, err := datalens.NewClient(config.Endpoint,
		datalens.WithToken(config.Token),
		datalens.WithApplication(config.Application),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	queries := buildExampleQueries(config)
	lines, err := runExample(context.Background(), client, queries)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, line := range lines {
		fmt.Println(line)
	}
}

func runExample(ctx context.Context, client tokenClient, queries exampleQueries) ([]string, error) {
	solana, err := client.QueryNative(ctx, queries.Solana)
	if err != nil {
		return nil, err
	}
	lines := formatNativeRows("solana-usdc", solana)

	tron, err := client.QueryNative(ctx, queries.Tron)
	if err != nil {
		return nil, err
	}
	lines = append(lines, formatNativeRows("tron-usdt", tron)...)

	return lines, nil
}

func buildRuntimeConfig(env map[string]string) (runtimeConfig, error) {
	config := runtimeConfig{
		Endpoint:    stringEnv(env, "DATALENS_ENDPOINT", "http://127.0.0.1:3000"),
		Token:       stringEnv(env, "DATALENS_TOKEN", ""),
		Application: stringEnv(env, "DATALENS_APPLICATION", "token-sdk-go"),
		Ethereum: rangeConfig{
			Start: intEnv(env, "DATALENS_ETHEREUM_FROM_BLOCK", 19000000),
			End:   intEnv(env, "DATALENS_ETHEREUM_TO_BLOCK", 19000010),
			First: intEnv(env, "DATALENS_ETHEREUM_FIRST", 10),
		},
		Solana: rangeConfig{
			Start: intEnv(env, "DATALENS_SOLANA_FROM_SLOT", 250000000),
			End:   intEnv(env, "DATALENS_SOLANA_TO_SLOT", 250000003),
		},
		Tron: rangeConfig{
			Start: intEnv(env, "DATALENS_TRON_FROM_BLOCK", 83200000),
			End:   intEnv(env, "DATALENS_TRON_TO_BLOCK", 83200002),
		},
	}
	return config, nil
}

func buildExampleQueries(config runtimeConfig) exampleQueries {
	chainID := 1
	return exampleQueries{
		Ethereum: datalens.EventFilter{
			Chain:     "ethereum",
			ChainID:   &chainID,
			Dataset:   "evm.logs",
			Address:   ethereumUSDCAddress,
			EventName: "Transfer",
			Signature: erc20TransferSignature,
			Topic0:    erc20TransferTopic0,
			FromBlock: intPtr(config.Ethereum.Start),
			ToBlock:   intPtr(config.Ethereum.End),
			First:     config.Ethereum.First,
		},
		Solana: datalens.QueryInput{
			Chain: datalens.ChainIdentity{
				Family:         datalens.ChainFamily{Kind: "other", Other: "solana"},
				ConfiguredName: "solana-mainnet-beta",
				NetworkID:      &datalens.NetworkID{Numeric: intPtr(101)},
			},
			DatasetKey: datalens.DatasetKey{Family: "solana", Name: "transactions"},
			Selector:   otherSelector("solana_address", "address", solanaUSDCMint, "solana-address"),
			Range: datalens.QueryRange{
				Kind:  "slot",
				Start: config.Solana.Start,
				End:   config.Solana.End,
			},
			Finality: "durable_only",
		},
		Tron: datalens.QueryInput{
			Chain: datalens.ChainIdentity{
				Family:         datalens.ChainFamily{Kind: "other", Other: "tron"},
				ConfiguredName: "tron-mainnet",
				NetworkID:      &datalens.NetworkID{Numeric: intPtr(728126428)},
			},
			DatasetKey: datalens.DatasetKey{Family: "tron", Name: "events"},
			Selector:   tronEventSelector(tronUSDTContractHex, "Transfer"),
			Range: datalens.QueryRange{
				Kind:  "block",
				Start: config.Tron.Start,
				End:   config.Tron.End,
			},
			Finality: "durable_only",
		},
	}
}

func formatDecodedTransfers(events []datalens.DecodedEvent) []string {
	lines := make([]string, 0, len(events))
	for _, event := range events {
		var decoded map[string]any
		if len(event.DecodedArgs) > 0 {
			_ = json.Unmarshal(event.DecodedArgs, &decoded)
		}
		lines = append(lines, strings.Join([]string{
			"ethereum-usdc",
			fmt.Sprintf("block=%d", event.BlockNumber),
			fmt.Sprintf("tx=%s", event.TransactionHash),
			fmt.Sprintf("log=%d", event.LogIndex),
			fmt.Sprintf("from=%s", stringValue(decoded["from"])),
			fmt.Sprintf("to=%s", stringValue(decoded["to"])),
			fmt.Sprintf("value=%s", stringValue(decoded["value"])),
		}, " "))
	}
	return lines
}

func formatNativeRows(label string, response datalens.QueryResponse) []string {
	lines := []string{fmt.Sprintf("%s cache=%s", label, string(response.Cache))}
	for _, row := range rowsFrom(response.Rows) {
		lines = append(lines, fmt.Sprintf("%s row=%s", label, string(row)))
	}
	return lines
}

func otherSelector(kind string, keyPrefix string, value string, fingerprintPrefix string) datalens.QuerySelector {
	return datalens.QuerySelector{
		Kind: "other",
		Other: &datalens.OtherSelector{
			Kind:         kind,
			Fingerprint:  fmt.Sprintf("%s/%s", fingerprintPrefix, digestPrefix(value, 8)),
			CanonicalKey: fmt.Sprintf("%s/%s", keyPrefix, value),
		},
	}
}

func tronEventSelector(contractHex string, eventName string) datalens.QuerySelector {
	canonicalKey := fmt.Sprintf("contracts/%s/events/%s", contractHex, eventName)
	return datalens.QuerySelector{
		Kind: "other",
		Other: &datalens.OtherSelector{
			Kind:         "tron_events",
			Fingerprint:  fmt.Sprintf("tron-events/%s", digestPrefix(canonicalKey, 12)),
			CanonicalKey: canonicalKey,
		},
	}
}

func rowsFrom(raw json.RawMessage) []json.RawMessage {
	var array []json.RawMessage
	if err := json.Unmarshal(raw, &array); err == nil {
		return array
	}
	var object struct {
		Rows []json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(raw, &object); err == nil {
		return object.Rows
	}
	return nil
}

func digestPrefix(value string, bytes int) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:bytes*2]
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		encoded, _ := json.Marshal(typed)
		return string(encoded)
	}
}

func osEnv() map[string]string {
	env := make(map[string]string)
	for _, item := range os.Environ() {
		key, value, found := strings.Cut(item, "=")
		if found {
			env[key] = value
		}
	}
	return env
}

func stringEnv(env map[string]string, name string, fallback string) string {
	value := strings.TrimSpace(env[name])
	if value == "" {
		return fallback
	}
	return value
}

func intEnv(env map[string]string, name string, fallback int) int {
	value := strings.TrimSpace(env[name])
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func intPtr(value int) *int {
	return &value
}
