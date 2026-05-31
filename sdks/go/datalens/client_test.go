package datalens

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodedEventsConnectionSendsBearerTokenAndGraphQLBody(t *testing.T) {
	var captured struct {
		Path          string
		Authorization string
		Application   string
		UserAgent     string
		Body          graphqlRequest
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.Path = r.URL.Path
		captured.Authorization = r.Header.Get("Authorization")
		captured.Application = r.Header.Get(ApplicationHeader)
		captured.UserAgent = r.Header.Get("User-Agent")
		if err := json.NewDecoder(r.Body).Decode(&captured.Body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		respondJSON(t, w, http.StatusOK, map[string]any{
			"data": map[string]any{
				"decodedEventsConnection": map[string]any{
					"nodes": []any{
						map[string]any{
							"dataset":         "evm.logs",
							"blockNumber":     12,
							"eventName":       "MessageSent",
							"transactionHash": "0xabc",
							"decodedArgs":     map[string]any{"sender": "0x1"},
							"payload":         map[string]any{"raw": true},
						},
					},
					"edges": []any{},
					"pageInfo": map[string]any{
						"endCursor":   "cursor-1",
						"hasNextPage": false,
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL,
		WithToken("secret-token"),
		WithApplication("query-app"),
		WithUserAgent("datalens-test/1.0"),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	page, err := client.DecodedEventsConnection(context.Background(), EventFilter{
		Dataset:   "evm.logs",
		EventName: "MessageSent",
		First:     25,
		After:     "before-cursor",
	})
	if err != nil {
		t.Fatalf("decoded events connection: %v", err)
	}

	if captured.Path != "/index/graphql" {
		t.Fatalf("path = %q, want /index/graphql", captured.Path)
	}
	if captured.Authorization != "Bearer secret-token" {
		t.Fatalf("authorization = %q", captured.Authorization)
	}
	if captured.Application != "query-app" {
		t.Fatalf("application = %q", captured.Application)
	}
	if captured.UserAgent != "datalens-test/1.0" {
		t.Fatalf("user agent = %q", captured.UserAgent)
	}
	if !strings.Contains(captured.Body.Query, "decodedEventsConnection") {
		t.Fatalf("query did not request decodedEventsConnection: %s", captured.Body.Query)
	}
	variables := captured.Body.Variables
	assertEqual(t, variables["dataset"], "evm.logs")
	assertEqual(t, variables["eventName"], "MessageSent")
	assertEqual[any](t, variables["first"], float64(25))
	assertEqual(t, variables["after"], "before-cursor")
	if len(page.Nodes) != 1 {
		t.Fatalf("decoded nodes length = %d", len(page.Nodes))
	}
	assertEqual(t, page.Nodes[0].EventName, "MessageSent")
	assertEqual(t, page.PageInfo.EndCursor, "cursor-1")
}

func TestFetchAllRawEventsPaginatesWithEndCursor(t *testing.T) {
	var afterValues []any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body graphqlRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		afterValues = append(afterValues, body.Variables["after"])
		cursor := "cursor-1"
		hasNext := len(afterValues) == 1
		block := 1
		if !hasNext {
			cursor = "cursor-2"
			block = 2
		}
		respondJSON(t, w, http.StatusOK, map[string]any{
			"data": map[string]any{
				"eventsConnection": map[string]any{
					"nodes": []any{
						map[string]any{
							"dataset":         "evm.logs",
							"blockNumber":     block,
							"transactionHash": "0xhash",
							"topics":          []any{"0xtopic"},
							"decoded":         map[string]any{},
							"payload":         map[string]any{},
						},
					},
					"edges": []any{},
					"pageInfo": map[string]any{
						"endCursor":   cursor,
						"hasNextPage": hasNext,
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	events, err := client.FetchAllRawEvents(context.Background(), EventFilter{Dataset: "evm.logs", First: 1})
	if err != nil {
		t.Fatalf("fetch all raw events: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("events length = %d", len(events))
	}
	if afterValues[0] != nil {
		t.Fatalf("first after = %#v, want nil", afterValues[0])
	}
	assertEqual(t, afterValues[1], "cursor-1")
	assertEqual(t, events[1].BlockNumber, 2)
}

func TestNativeDiscoveryUsesNativeGraphQLEndpoint(t *testing.T) {
	var captured graphqlRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/native/graphql" {
			t.Fatalf("path = %q, want /native/graphql", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		respondJSON(t, w, http.StatusOK, map[string]any{
			"data": map[string]any{
				"discovery": map[string]any{
					"chains": []any{
						map[string]any{
							"identity": map[string]any{"configuredName": "ethereum"},
							"datasets": []any{
								map[string]any{
									"datasetKey": "evm.logs",
									"rangeKinds": map[string]any{"block": true},
									"selectors":  []any{"all", "evm_logs"},
									"enabled":    true,
								},
							},
						},
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	discovery, err := client.Discovery(context.Background())
	if err != nil {
		t.Fatalf("discovery: %v", err)
	}

	if !strings.Contains(captured.Query, "discovery") {
		t.Fatalf("query did not request discovery: %s", captured.Query)
	}
	if len(discovery.Chains) != 1 {
		t.Fatalf("chains length = %d", len(discovery.Chains))
	}
	assertEqual(t, discovery.Chains[0].Datasets[0].DatasetKey, "evm.logs")
}

func TestGraphQLErrorsExposeStableKind(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(t, w, http.StatusOK, map[string]any{
			"errors": []any{
				map[string]any{
					"message": "unsupported dataset",
					"extensions": map[string]any{
						"code": "UnsupportedDataset",
						"kind": "unsupported_dataset",
					},
				},
			},
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = client.RawEventsConnection(context.Background(), EventFilter{Dataset: "evm.blocks"})
	if err == nil {
		t.Fatal("expected error")
	}
	var gqlErr *GraphQLError
	if !errors.As(err, &gqlErr) {
		t.Fatalf("error type = %T, want *GraphQLError", err)
	}
	assertEqual(t, gqlErr.Errors[0].Kind, "unsupported_dataset")
	assertEqual(t, gqlErr.Errors[0].Code, "UnsupportedDataset")
}

func TestHTTPAuthAndRateLimitErrorsUseTypedErrors(t *testing.T) {
	statuses := []int{http.StatusInternalServerError, http.StatusUnauthorized, http.StatusTooManyRequests}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				respondJSON(t, w, status, map[string]any{"error": map[string]any{"kind": "rate_limited", "message": "limited"}})
			}))
			defer server.Close()

			client, err := NewClient(server.URL)
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			_, err = client.RawEventsConnection(context.Background(), EventFilter{Dataset: "evm.logs"})
			if err == nil {
				t.Fatal("expected error")
			}
			switch status {
			case http.StatusInternalServerError:
				var httpErr *HTTPError
				if !errors.As(err, &httpErr) {
					t.Fatalf("error type = %T, want *HTTPError", err)
				}
			case http.StatusUnauthorized:
				var authErr *AuthError
				if !errors.As(err, &authErr) {
					t.Fatalf("error type = %T, want *AuthError", err)
				}
			case http.StatusTooManyRequests:
				var rateErr *RateLimitError
				if !errors.As(err, &rateErr) {
					t.Fatalf("error type = %T, want *RateLimitError", err)
				}
			}
		})
	}
}

func respondJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}
