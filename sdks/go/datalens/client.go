package datalens

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	ApplicationHeader = "x-datalens-application"
	DefaultUserAgent  = "datalens-go/0.1"
)

const (
	defaultApplication       = "unknown"
	defaultIndexGraphQLPath  = "/index/graphql"
	defaultNativeGraphQLPath = "/native/graphql"
)

type Client struct {
	endpoint          string
	token             string
	application       string
	userAgent         string
	indexGraphQLPath  string
	nativeGraphQLPath string
	httpClient        *http.Client
}

type Option func(*Client)

func NewClient(endpoint string, options ...Option) (*Client, error) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("datalens endpoint must not be empty")
	}
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return nil, fmt.Errorf("parse datalens endpoint: %w", err)
	}
	client := &Client{
		endpoint:          endpoint,
		application:       defaultApplication,
		userAgent:         DefaultUserAgent,
		indexGraphQLPath:  defaultIndexGraphQLPath,
		nativeGraphQLPath: defaultNativeGraphQLPath,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	for _, option := range options {
		option(client)
	}
	if strings.TrimSpace(client.application) == "" {
		client.application = defaultApplication
	}
	if client.userAgent == "" {
		client.userAgent = DefaultUserAgent
	}
	return client, nil
}

func WithToken(token string) Option {
	return func(client *Client) {
		client.token = strings.TrimSpace(token)
	}
}

func WithApplication(application string) Option {
	return func(client *Client) {
		client.application = strings.TrimSpace(application)
	}
}

func WithUserAgent(userAgent string) Option {
	return func(client *Client) {
		client.userAgent = strings.TrimSpace(userAgent)
	}
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(client *Client) {
		if httpClient != nil {
			client.httpClient = httpClient
		}
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(client *Client) {
		transport := http.DefaultTransport
		if client.httpClient != nil && client.httpClient.Transport != nil {
			transport = client.httpClient.Transport
		}
		client.httpClient = &http.Client{
			Transport: transport,
			Timeout:   timeout,
		}
	}
}

func WithIndexGraphQLPath(path string) Option {
	return func(client *Client) {
		if strings.TrimSpace(path) != "" {
			client.indexGraphQLPath = normalizePath(path)
		}
	}
}

func WithNativeGraphQLPath(path string) Option {
	return func(client *Client) {
		if strings.TrimSpace(path) != "" {
			client.nativeGraphQLPath = normalizePath(path)
		}
	}
}

func (c *Client) RawEvents(ctx context.Context, filter EventFilter) ([]IndexedEvent, error) {
	var response struct {
		Events []IndexedEvent `json:"events"`
	}
	err := c.graphql(ctx, c.indexGraphQLPath, rawEventsQuery, filter.variables(false), &response)
	return response.Events, err
}

func (c *Client) DecodedEvents(ctx context.Context, filter EventFilter) ([]DecodedEvent, error) {
	var response struct {
		DecodedEvents []DecodedEvent `json:"decodedEvents"`
	}
	err := c.graphql(ctx, c.indexGraphQLPath, decodedEventsQuery, filter.variables(false), &response)
	return response.DecodedEvents, err
}

func (c *Client) RawEventsConnection(ctx context.Context, filter EventFilter) (IndexedEventConnection, error) {
	var response struct {
		EventsConnection IndexedEventConnection `json:"eventsConnection"`
	}
	err := c.graphql(ctx, c.indexGraphQLPath, rawEventsConnectionQuery, filter.variables(true), &response)
	return response.EventsConnection, err
}

func (c *Client) DecodedEventsConnection(ctx context.Context, filter EventFilter) (DecodedEventConnection, error) {
	var response struct {
		DecodedEventsConnection DecodedEventConnection `json:"decodedEventsConnection"`
	}
	err := c.graphql(ctx, c.indexGraphQLPath, decodedEventsConnectionQuery, filter.variables(true), &response)
	return response.DecodedEventsConnection, err
}

func (c *Client) FetchAllRawEvents(ctx context.Context, filter EventFilter) ([]IndexedEvent, error) {
	var events []IndexedEvent
	for {
		page, err := c.RawEventsConnection(ctx, filter)
		if err != nil {
			return nil, err
		}
		events = append(events, page.Nodes...)
		if !page.PageInfo.HasNextPage {
			return events, nil
		}
		filter.After = page.PageInfo.EndCursor
	}
}

func (c *Client) FetchAllDecodedEvents(ctx context.Context, filter EventFilter) ([]DecodedEvent, error) {
	var events []DecodedEvent
	for {
		page, err := c.DecodedEventsConnection(ctx, filter)
		if err != nil {
			return nil, err
		}
		events = append(events, page.Nodes...)
		if !page.PageInfo.HasNextPage {
			return events, nil
		}
		filter.After = page.PageInfo.EndCursor
	}
}

func (c *Client) Chains(ctx context.Context) ([]string, error) {
	var response struct {
		Chains []string `json:"chains"`
	}
	err := c.graphql(ctx, c.nativeGraphQLPath, chainsQuery, nil, &response)
	return response.Chains, err
}

func (c *Client) Discovery(ctx context.Context) (Discovery, error) {
	var response struct {
		Discovery Discovery `json:"discovery"`
	}
	err := c.graphql(ctx, c.nativeGraphQLPath, discoveryQuery, nil, &response)
	return response.Discovery, err
}

func (c *Client) QueryNative(ctx context.Context, input QueryInput) (QueryResponse, error) {
	var response struct {
		Query QueryResponse `json:"query"`
	}
	err := c.graphql(ctx, c.nativeGraphQLPath, nativeQuery, map[string]any{"input": input}, &response)
	return response.Query, err
}

type graphqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type graphqlResponse struct {
	Data   json.RawMessage    `json:"data"`
	Errors []GraphQLErrorItem `json:"errors"`
}

func (c *Client) graphql(ctx context.Context, path string, query string, variables map[string]any, out any) error {
	body, err := json.Marshal(graphqlRequest{
		Query:     query,
		Variables: variables,
	})
	if err != nil {
		return fmt.Errorf("encode datalens GraphQL request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build datalens GraphQL request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set(ApplicationHeader, c.application)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send datalens GraphQL request: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read datalens GraphQL response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return classifyHTTPError(resp.StatusCode, responseBody)
	}

	var envelope graphqlResponse
	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		return fmt.Errorf("decode datalens GraphQL response: %w", err)
	}
	if len(envelope.Errors) > 0 {
		return classifyGraphQLError(envelope.Errors)
	}
	if out == nil {
		return nil
	}
	if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return nil
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		return fmt.Errorf("decode datalens GraphQL data: %w", err)
	}
	return nil
}

func normalizePath(path string) string {
	path = strings.TrimSpace(path)
	if strings.HasPrefix(path, "/") {
		return path
	}
	return "/" + path
}
