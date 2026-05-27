package dynatrace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/httputil"
)

type Client interface {
	IngestBizevent(ctx context.Context, eventType string, attrs map[string]any) error
	QueryConsumerHealth(ctx context.Context, syncStartMs, syncEndMs int64) ([]ConsumerHealth, error)
}

type ConsumerHealth struct {
	Consumer     string
	ErrorRate    float64
	LatencyP95Ms float64
}

type DynatraceError struct {
	StatusCode int
	Body       string
}

func (e *DynatraceError) Error() string {
	return fmt.Sprintf("dynatrace: HTTP %d: %s", e.StatusCode, e.Body)
}

var (
	ErrTelemetryUnavailable = errors.New("dynatrace: telemetry endpoint unavailable")
	ErrCorrelationMissing   = errors.New("dynatrace: correlation token missing from context")
)

const (
	bizeventIngestPath = "/api/v2/bizevents/ingest"
	// Placeholder path. Verify against the Dynatrace Grail DQL execute API before production.
	dqlQueryPath = "/api/v2/dql/query"
	eventSource  = "kineticz"
)

type client struct {
	http    *http.Client
	audit   audit.Writer
	baseURL string
	token   string
	backoff time.Duration
	retries int
}

// NewClient constructs a Dynatrace REST client. token is the API access
// token (DYNATRACE_TOKEN env var); the do helper attaches it as
// "Authorization: Bearer <token>" on every request.
//
// [unverified] Dynatrace's actual API auth prefix is "Api-Token <token>"
// per their public docs. Bearer is honored here per Phase 10 spec; flip
// the prefix in c.do below if production rejects with 401.
func NewClient(httpClient *http.Client, aw audit.Writer, baseURL, token string) *client {
	return &client{
		http:    httpClient,
		audit:   aw,
		baseURL: baseURL,
		token:   token,
		backoff: 100 * time.Millisecond,
		retries: 3,
	}
}

func (c *client) IngestBizevent(ctx context.Context, eventType string, attrs map[string]any) error {
	token, ok := corr.FromContext(ctx)
	if !ok || token == "" {
		return ErrCorrelationMissing
	}
	event := map[string]any{
		"id":                         string(token),
		"type":                       eventType,
		"source":                     eventSource,
		"kineticz.correlation_token": string(token),
		"data":                       attrs,
	}
	buf, err := json.Marshal([]any{event})
	if err != nil {
		return fmt.Errorf("dynatrace: marshal bizevent: %w", err)
	}
	body, status, err := c.do(ctx, http.MethodPost, bizeventIngestPath, buf)
	if err != nil {
		_ = c.recordAudit(ctx, "DYNATRACE_INGEST_FAILED", eventType, err)
		return err
	}
	if status >= 400 {
		e := &DynatraceError{StatusCode: status, Body: string(body)}
		_ = c.recordAudit(ctx, "DYNATRACE_INGEST_FAILED", eventType, e)
		return e
	}
	if err := c.recordAudit(ctx, "DYNATRACE_INGEST_OK", eventType, nil); err != nil {
		return fmt.Errorf("dynatrace: audit write failed: %w", err)
	}
	return nil
}

func (c *client) QueryConsumerHealth(ctx context.Context, syncStartMs, syncEndMs int64) ([]ConsumerHealth, error) {
	if _, ok := corr.FromContext(ctx); !ok {
		return nil, ErrCorrelationMissing
	}
	reqBody := map[string]any{
		"query":                 "fetch bizevents | filter event.type == \"sync.complete\" | summarize avg(error_rate), percentile(latency_ms, 95) by consumer",
		"defaultTimeframeStart": time.UnixMilli(syncStartMs).UTC().Format(time.RFC3339),
		"defaultTimeframeEnd":   time.UnixMilli(syncEndMs).UTC().Format(time.RFC3339),
	}
	buf, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("dynatrace: marshal dql: %w", err)
	}
	body, status, err := c.do(ctx, http.MethodPost, dqlQueryPath, buf)
	if err != nil {
		_ = c.recordAudit(ctx, "DYNATRACE_DQL_FAILED", "consumer_health", err)
		return nil, err
	}
	if status >= 400 {
		e := &DynatraceError{StatusCode: status, Body: string(body)}
		_ = c.recordAudit(ctx, "DYNATRACE_DQL_FAILED", "consumer_health", e)
		return nil, e
	}
	var res struct {
		State  string `json:"state"`
		Result struct {
			Records []struct {
				Consumer     string  `json:"consumer"`
				ErrorRate    float64 `json:"error_rate"`
				LatencyP95Ms float64 `json:"latency_p95_ms"`
			} `json:"records"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("dynatrace: decode dql: %w", err)
	}
	out := make([]ConsumerHealth, 0, len(res.Result.Records))
	for _, r := range res.Result.Records {
		out = append(out, ConsumerHealth{
			Consumer:     r.Consumer,
			ErrorRate:    r.ErrorRate,
			LatencyP95Ms: r.LatencyP95Ms,
		})
	}
	if err := c.recordAudit(ctx, "DYNATRACE_DQL_OK", "consumer_health", nil); err != nil {
		return nil, fmt.Errorf("dynatrace: audit write failed: %w", err)
	}
	return out, nil
}

func (c *client) recordAudit(ctx context.Context, action, scope string, callErr error) error {
	payload := map[string]any{"scope": scope}
	if callErr != nil {
		payload["error"] = callErr.Error()
	}
	body, _ := json.Marshal(payload)
	return c.audit.Append(ctx, action, body)
}

func (c *client) do(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, 0, fmt.Errorf("dynatrace: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := httputil.Do(ctx, c.http, req, c.retries, c.backoff)
	if err != nil {
		if errors.Is(err, httputil.ErrUnavailable) {
			return nil, 0, fmt.Errorf("%w: %v", ErrTelemetryUnavailable, err)
		}
		return nil, 0, err
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("dynatrace: read body: %w", err)
	}
	return buf, resp.StatusCode, nil
}
