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
	backoff time.Duration
	retries int
}

func NewClient(httpClient *http.Client, aw audit.Writer, baseURL string) *client {
	return &client{
		http:    httpClient,
		audit:   aw,
		baseURL: baseURL,
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
	url := c.baseURL + path
	var lastErr error
	delay := c.backoff
	for attempt := 0; attempt < c.retries; attempt++ {
		var req *http.Request
		var err error
		if body != nil {
			req, err = http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
			}
		} else {
			req, err = http.NewRequestWithContext(ctx, method, url, nil)
		}
		if err != nil {
			return nil, 0, fmt.Errorf("dynatrace: build request: %w", err)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
		} else {
			buf, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				lastErr = readErr
			} else if resp.StatusCode >= 500 {
				lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(buf))
			} else {
				return buf, resp.StatusCode, nil
			}
		}
		if attempt+1 >= c.retries {
			break
		}
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-time.After(delay):
			delay *= 2
		}
	}
	return nil, 0, fmt.Errorf("%w: %v", ErrTelemetryUnavailable, lastErr)
}
