package datalens

import (
	"encoding/json"
	"fmt"
	"strings"
)

type GraphQLError struct {
	Errors []GraphQLErrorItem
}

func (e *GraphQLError) Error() string {
	if len(e.Errors) == 0 {
		return "datalens GraphQL error"
	}
	return fmt.Sprintf("datalens GraphQL error: %s", e.Errors[0].Message)
}

type GraphQLErrorItem struct {
	Message    string                 `json:"message"`
	Path       []any                  `json:"path,omitempty"`
	Locations  []GraphQLErrorLocation `json:"locations,omitempty"`
	Extensions GraphQLErrorExtension  `json:"extensions,omitempty"`
	Code       string                 `json:"-"`
	Kind       string                 `json:"-"`
}

func (e *GraphQLErrorItem) UnmarshalJSON(data []byte) error {
	type alias GraphQLErrorItem
	var item alias
	if err := json.Unmarshal(data, &item); err != nil {
		return err
	}
	item.Code = item.Extensions.Code
	item.Kind = item.Extensions.Kind
	*e = GraphQLErrorItem(item)
	return nil
}

type GraphQLErrorExtension struct {
	Code string `json:"code,omitempty"`
	Kind string `json:"kind,omitempty"`
}

type GraphQLErrorLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

type HTTPError struct {
	StatusCode int
	Kind       string
	Message    string
	Body       []byte
}

func (e *HTTPError) Error() string {
	if e.Kind != "" || e.Message != "" {
		return fmt.Sprintf("datalens HTTP error %d %s: %s", e.StatusCode, e.Kind, e.Message)
	}
	return fmt.Sprintf("datalens HTTP error %d", e.StatusCode)
}

type AuthError struct {
	*HTTPError
	GraphQL *GraphQLError
}

func (e *AuthError) Error() string {
	if e.HTTPError != nil {
		return e.HTTPError.Error()
	}
	return e.GraphQL.Error()
}

type RateLimitError struct {
	*HTTPError
	GraphQL *GraphQLError
}

func (e *RateLimitError) Error() string {
	if e.HTTPError != nil {
		return e.HTTPError.Error()
	}
	return e.GraphQL.Error()
}

type httpErrorBody struct {
	Error struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	} `json:"error"`
}

func classifyHTTPError(status int, body []byte) error {
	httpErr := &HTTPError{
		StatusCode: status,
		Body:       body,
	}
	var errorBody httpErrorBody
	if err := json.Unmarshal(body, &errorBody); err == nil {
		httpErr.Kind = errorBody.Error.Kind
		httpErr.Message = errorBody.Error.Message
	}
	switch status {
	case 401, 403:
		return &AuthError{HTTPError: httpErr}
	case 429:
		return &RateLimitError{HTTPError: httpErr}
	default:
		return httpErr
	}
}

func classifyGraphQLError(items []GraphQLErrorItem) error {
	gqlErr := &GraphQLError{Errors: items}
	for _, item := range items {
		code := strings.ToLower(item.Code)
		kind := strings.ToLower(item.Kind)
		switch {
		case code == "authenticationfailed" || code == "unauthorized" || kind == "authentication_failed" || kind == "unauthorized":
			return &AuthError{GraphQL: gqlErr}
		case code == "ratelimited" || kind == "rate_limited":
			return &RateLimitError{GraphQL: gqlErr}
		}
	}
	return gqlErr
}
