package httputil

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// ErrUnavailable wraps the last underlying error after retries are exhausted.
// Callers should errors.Is against this to detect retry-exhaustion vs. one-shot
// errors (e.g., context cancellation, request-build failures).
var ErrUnavailable = errors.New("httputil: service unavailable after retries")

// Do executes req against client with exponential-backoff retries on 5xx
// responses and transport errors. 4xx and 2xx responses return immediately.
// The request body must be rebuildable via req.GetBody for retry attempts to
// send a fresh body; http.NewRequestWithContext auto-sets GetBody for
// *bytes.Buffer, *bytes.Reader, and *strings.Reader bodies. Backoff starts at
// the given duration and doubles between attempts.
func Do(ctx context.Context, client *http.Client, req *http.Request, maxRetries int, backoff time.Duration) (*http.Response, error) {
	if maxRetries < 1 {
		maxRetries = 1
	}
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}

	var lastErr error
	delay := backoff
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 && req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("httputil: get body for retry: %w", err)
			}
			req.Body = body
		}

		resp, err := client.Do(req.WithContext(ctx))
		if err == nil {
			if resp.StatusCode < 500 {
				return resp, nil
			}
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
		} else {
			lastErr = err
		}

		if attempt+1 >= maxRetries {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
			delay *= 2
		}
	}
	return nil, fmt.Errorf("%w: %v", ErrUnavailable, lastErr)
}
