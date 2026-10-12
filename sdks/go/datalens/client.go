package datalens

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// RetryConfig retries one call. The delay doubles from InitialBackoff up to MaxBackoff,
// scaled by a random factor in [0.5, 1). Retry-After overrides it.
type RetryConfig struct {
	// MaxAttempts counts the first try. 1 disables retries.
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

var DefaultRetry = RetryConfig{MaxAttempts: 4, InitialBackoff: 250 * time.Millisecond, MaxBackoff: 10 * time.Second}

// Backoff is the jittered delay before retry number failures, counting from 1.
func (r RetryConfig) Backoff(failures int) time.Duration {
	delay := r.InitialBackoff << min(max(failures-1, 0), 20)
	if delay > r.MaxBackoff || delay <= 0 {
		delay = r.MaxBackoff
	}
	return time.Duration(float64(delay) * (0.5 + rand.Float64()/2))
}

type ClientConfig struct {
	// Endpoint is a base URL such as https://datalens.example.com.
	Endpoint    string
	Application string
	Token       string
	// Timeout is per request. A heads long-poll adds its own wait on top. Zero means 30 s.
	Timeout time.Duration
	// Retry zero value means DefaultRetry.
	Retry      RetryConfig
	HTTPClient *http.Client
}

// Client is one application's view of a datalens server. It is safe for concurrent use.
type Client struct {
	endpoint    string
	application string
	token       string
	timeout     time.Duration
	retry       RetryConfig
	http        *http.Client
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		endpoint:    strings.TrimRight(config.Endpoint, "/"),
		application: config.Application,
		token:       config.Token,
		timeout:     config.Timeout,
		retry:       config.Retry,
		http:        config.HTTPClient,
	}
	if client.timeout == 0 {
		client.timeout = 30 * time.Second
	}
	if client.retry.MaxAttempts == 0 {
		client.retry = DefaultRetry
	}
	if client.http == nil {
		client.http = http.DefaultClient
	}
	return client
}

// Query returns one page. Page.NextCursor names the next page of the same request.
func (c *Client) Query(ctx context.Context, request *QueryRequest) (*QueryResponse, error) {
	var response QueryResponse
	err := c.call(ctx, http.MethodPost, "/v1/query", request, 0, &response)
	return &response, err
}

// QueryAll returns every page of request with rows concatenated. The other fields come
// from the last page, whose Served contains the rows of every earlier page.
func (c *Client) QueryAll(ctx context.Context, request *QueryRequest) (*QueryResponse, error) {
	merged, err := c.Query(ctx, request)
	if err != nil {
		return nil, err
	}
	rows := merged.Rows
	paged := *request
	for merged.Page.NextCursor != nil {
		page := PageRequest{Cursor: *merged.Page.NextCursor}
		if request.Page != nil {
			page.Limit = request.Page.Limit
		}
		paged.Page = &page
		if merged, err = c.Query(ctx, &paged); err != nil {
			return nil, err
		}
		rows = append(rows, merged.Rows...)
	}
	merged.Rows = rows
	return merged, nil
}

func (c *Client) Heads(ctx context.Context, chain string) (*ChainHeads, error) {
	var heads ChainHeads
	err := c.call(ctx, http.MethodGet, "/v1/heads/"+url.PathEscape(chain), nil, 0, &heads)
	return &heads, err
}

// AllHeads returns the heads of every chain this application may read.
func (c *Client) AllHeads(ctx context.Context) ([]ChainHeads, error) {
	var response struct {
		Chains []ChainHeads `json:"chains"`
	}
	err := c.call(ctx, http.MethodGet, "/v1/heads", nil, 0, &response)
	return response.Chains, err
}

// WaitHeads returns once head kind passes after, or with the unchanged heads after timeout.
// The server caps timeout at 60 s.
func (c *Client) WaitHeads(ctx context.Context, chain string, after uint64, kind HeadKind, timeout time.Duration) (*ChainHeads, error) {
	query := url.Values{
		"after":      {strconv.FormatUint(after, 10)},
		"kind":       {string(kind)},
		"timeout_ms": {strconv.FormatInt(timeout.Milliseconds(), 10)},
	}
	var heads ChainHeads
	path := "/v1/heads/" + url.PathEscape(chain) + "/wait?" + query.Encode()
	err := c.call(ctx, http.MethodGet, path, nil, timeout, &heads)
	return &heads, err
}

// Watch subscribes the selector for the server's TTL, or renews it.
func (c *Client) Watch(ctx context.Context, request *WatchRequest) (*WatchResponse, error) {
	var response WatchResponse
	err := c.call(ctx, http.MethodPost, "/v1/watch", request, 0, &response)
	return &response, err
}

func (c *Client) Unwatch(ctx context.Context, request *WatchRequest) error {
	return c.call(ctx, http.MethodDelete, "/v1/watch", request, 0, nil)
}

func (c *Client) call(ctx context.Context, method, path string, body any, extra time.Duration, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return &Error{Type: DecodeError, Message: err.Error(), Err: err}
		}
	}
	for failures := 1; ; failures++ {
		err := c.once(ctx, method, path, payload, extra, out)
		var failure *Error
		if err == nil || !errors.As(err, &failure) || !failure.Retryable() || failures >= c.retry.MaxAttempts {
			return err
		}
		delay := failure.RetryAfter
		if delay == 0 {
			delay = c.retry.Backoff(failures)
		}
		if err := sleep(ctx, delay); err != nil {
			return err
		}
	}
}

func (c *Client) once(ctx context.Context, method, path string, payload []byte, extra time.Duration, out any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout+extra)
	defer cancel()
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, body)
	if err != nil {
		return &Error{Type: HTTPError, Message: err.Error(), Err: err}
	}
	request.Header.Set("authorization", "Bearer "+c.token)
	request.Header.Set("x-datalens-application", c.application)
	if payload != nil {
		request.Header.Set("content-type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
			return ctx.Err()
		}
		return &Error{Type: HTTPError, Message: err.Error(), Err: err}
	}
	defer response.Body.Close()
	text, err := io.ReadAll(response.Body)
	if err != nil {
		return &Error{Type: HTTPError, Message: err.Error(), Err: err}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return apiError(response, text)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(text, out); err != nil {
		return &Error{Type: DecodeError, Message: err.Error(), Err: err}
	}
	return nil
}

func apiError(response *http.Response, text []byte) *Error {
	failure := &Error{Type: APIError, Kind: "http", Message: string(text), Status: response.StatusCode}
	var body struct {
		Error *struct {
			Kind    string `json:"kind"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(text, &body) == nil && body.Error != nil && body.Error.Kind != "" {
		failure.Kind = body.Error.Kind
		failure.Message = body.Error.Message
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(response.Header.Get("retry-after"))); err == nil && seconds >= 0 {
		failure.RetryAfter = time.Duration(seconds) * time.Second
	}
	return failure
}

func sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("datalens: %w", ctx.Err())
	}
}
