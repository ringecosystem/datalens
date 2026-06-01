package main

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/helixbox/datalens/sdks/go/datalens"
)

type recordingTokenClient struct {
	calls []string
}

func (client *recordingTokenClient) DecodedEventsConnection(context.Context, datalens.EventFilter) (datalens.DecodedEventConnection, error) {
	client.calls = append(client.calls, "index")
	return datalens.DecodedEventConnection{}, nil
}

func (client *recordingTokenClient) QueryNative(_ context.Context, input datalens.QueryInput) (datalens.QueryResponse, error) {
	client.calls = append(client.calls, input.DatasetKey.Family+"."+input.DatasetKey.Name)
	return datalens.QueryResponse{
		Cache: json.RawMessage(`{"outcome":"miss"}`),
		Rows:  json.RawMessage(`[]`),
	}, nil
}

func TestRunExampleUsesNativeQueriesOnlyForLiveSmoke(t *testing.T) {
	client := &recordingTokenClient{}
	queries := buildExampleQueries(runtimeConfig{
		Endpoint:    "http://127.0.0.1:3000",
		Application: "token-sdk-go",
		Ethereum:    rangeConfig{Start: 19000000, End: 19000010, First: 3},
		Solana:      rangeConfig{Start: 250000000, End: 250000003},
		Tron:        rangeConfig{Start: 60000000, End: 60000002},
	})

	lines, err := runExample(context.Background(), client, queries)
	if err != nil {
		t.Fatalf("run example: %v", err)
	}

	wantCalls := []string{"solana.transactions", "tron.events"}
	if !reflect.DeepEqual(client.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", client.calls, wantCalls)
	}
	wantLines := []string{
		`solana-usdc cache={"outcome":"miss"}`,
		`tron-usdt cache={"outcome":"miss"}`,
	}
	if !reflect.DeepEqual(lines, wantLines) {
		t.Fatalf("lines = %#v, want %#v", lines, wantLines)
	}
}

func TestBuildExampleQueriesUsesOfficialTokenTargetsAndBoundedRanges(t *testing.T) {
	queries := buildExampleQueries(runtimeConfig{
		Endpoint:    "http://127.0.0.1:3000",
		Application: "token-sdk-go",
		Ethereum:    rangeConfig{Start: 19000000, End: 19000010, First: 3},
		Solana:      rangeConfig{Start: 250000000, End: 250000003},
		Tron:        rangeConfig{Start: 60000000, End: 60000002},
	})

	chainID := 1
	wantEthereum := datalens.EventFilter{
		Chain:     "ethereum",
		ChainID:   &chainID,
		Dataset:   "evm.logs",
		Address:   ethereumUSDCAddress,
		EventName: "Transfer",
		Signature: erc20TransferSignature,
		Topic0:    erc20TransferTopic0,
		FromBlock: intPtr(19000000),
		ToBlock:   intPtr(19000010),
		First:     3,
	}
	if !reflect.DeepEqual(queries.Ethereum, wantEthereum) {
		t.Fatalf("ethereum query = %#v, want %#v", queries.Ethereum, wantEthereum)
	}

	wantSolana := datalens.QueryInput{
		Chain: datalens.ChainIdentity{
			Family:         datalens.ChainFamily{Kind: "other", Other: "solana"},
			ConfiguredName: "solana-mainnet-beta",
			NetworkID:      &datalens.NetworkID{Numeric: intPtr(101)},
		},
		DatasetKey: datalens.DatasetKey{Family: "solana", Name: "transactions"},
		Selector: datalens.QuerySelector{
			Kind: "other",
			Other: &datalens.OtherSelector{
				Kind:         "solana_address",
				Fingerprint:  "solana-address/f249bbf137c2e667",
				CanonicalKey: "address/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
			},
		},
		Range:    datalens.QueryRange{Kind: "slot", Start: 250000000, End: 250000003},
		Finality: "durable_only",
	}
	if !reflect.DeepEqual(queries.Solana, wantSolana) {
		t.Fatalf("solana query = %#v, want %#v", queries.Solana, wantSolana)
	}

	wantTron := datalens.QueryInput{
		Chain: datalens.ChainIdentity{
			Family:         datalens.ChainFamily{Kind: "other", Other: "tron"},
			ConfiguredName: "tron-mainnet",
			NetworkID:      &datalens.NetworkID{Numeric: intPtr(728126428)},
		},
		DatasetKey: datalens.DatasetKey{Family: "tron", Name: "events"},
		Selector: datalens.QuerySelector{
			Kind: "other",
			Other: &datalens.OtherSelector{
				Kind:         "tron_events",
				Fingerprint:  "tron-events/8b35d4984847524df4944061",
				CanonicalKey: "contracts/41a614f803b6fd780986a42c78ec9c7f77e6ded13c/events/Transfer",
			},
		},
		Range:    datalens.QueryRange{Kind: "block", Start: 60000000, End: 60000002},
		Finality: "durable_only",
	}
	if !reflect.DeepEqual(queries.Tron, wantTron) {
		t.Fatalf("tron query = %#v, want %#v", queries.Tron, wantTron)
	}
}

func TestRuntimeConfigReadsEndpointTokenApplicationAndRanges(t *testing.T) {
	config, err := buildRuntimeConfig(map[string]string{
		"DATALENS_ENDPOINT":            "http://datalens.example",
		"DATALENS_TOKEN":               "secret-token",
		"DATALENS_APPLICATION":         "demo-app",
		"DATALENS_ETHEREUM_FROM_BLOCK": "19100000",
		"DATALENS_ETHEREUM_TO_BLOCK":   "19100001",
		"DATALENS_ETHEREUM_FIRST":      "2",
		"DATALENS_SOLANA_FROM_SLOT":    "251000000",
		"DATALENS_SOLANA_TO_SLOT":      "251000005",
		"DATALENS_TRON_FROM_BLOCK":     "60100000",
		"DATALENS_TRON_TO_BLOCK":       "60100004",
	})
	if err != nil {
		t.Fatalf("build config: %v", err)
	}

	want := runtimeConfig{
		Endpoint:    "http://datalens.example",
		Token:       "secret-token",
		Application: "demo-app",
		Ethereum:    rangeConfig{Start: 19100000, End: 19100001, First: 2},
		Solana:      rangeConfig{Start: 251000000, End: 251000005},
		Tron:        rangeConfig{Start: 60100000, End: 60100004},
	}
	if !reflect.DeepEqual(config, want) {
		t.Fatalf("config = %#v, want %#v", config, want)
	}
}

func TestFormattersPrintNormalizedEventAndCacheSummaries(t *testing.T) {
	event := datalens.DecodedEvent{
		BlockNumber:     19000000,
		TransactionHash: "0xabc",
		LogIndex:        1,
		DecodedArgs:     json.RawMessage(`{"from":"0x1111111111111111111111111111111111111111","to":"0x2222222222222222222222222222222222222222","value":"1000000"}`),
	}
	decoded := formatDecodedTransfers([]datalens.DecodedEvent{event})
	wantDecoded := []string{
		"ethereum-usdc block=19000000 tx=0xabc log=1 from=0x1111111111111111111111111111111111111111 to=0x2222222222222222222222222222222222222222 value=1000000",
	}
	if !reflect.DeepEqual(decoded, wantDecoded) {
		t.Fatalf("decoded = %#v, want %#v", decoded, wantDecoded)
	}

	response := datalens.QueryResponse{
		Cache: json.RawMessage(`{"outcome":"hit","hit_ranges":[{"start":250000000,"end":250000003}]}`),
		Rows:  json.RawMessage(`{"rows":[{"slot":250000001,"signature":"5sig","account":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","post_amount":"42"}]}`),
	}
	native := formatNativeRows("solana-usdc", response)
	wantNative := []string{
		`solana-usdc cache={"outcome":"hit","hit_ranges":[{"start":250000000,"end":250000003}]}`,
		`solana-usdc row={"slot":250000001,"signature":"5sig","account":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","post_amount":"42"}`,
	}
	if !reflect.DeepEqual(native, wantNative) {
		t.Fatalf("native = %#v, want %#v", native, wantNative)
	}
}
