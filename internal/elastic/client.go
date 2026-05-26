package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/skunkworks0x/kineticz/internal/audit"
)

type Client interface {
	LookupContract(ctx context.Context, q ContractQuery) (*ContractContext, error)
}

type ContractQuery struct {
	ContractName  string
	Columns       []string
	DiffEmbedding []float32
}

type ContractContext struct {
	YAMLDefinition     string
	MitigationPatterns []Mitigation
	RRFConfidence      float64
}

type Mitigation struct {
	DiffID  string
	Score   float64
	Summary string
}

type ElasticError struct {
	StatusCode int
	Body       string
}

func (e *ElasticError) Error() string {
	return fmt.Sprintf("elastic: HTTP %d: %s", e.StatusCode, e.Body)
}

var (
	ErrContractNotFound   = errors.New("elastic: contract not found")
	ErrElasticUnavailable = errors.New("elastic: service unavailable")
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

func (c *client) LookupContract(ctx context.Context, q ContractQuery) (*ContractContext, error) {
	yamlDef, err := c.fetchContractYAML(ctx, q.ContractName)
	if err != nil {
		_ = c.recordAudit(ctx, "ELASTIC_LOOKUP_FAILED", q.ContractName, err)
		return nil, err
	}
	hits, err := c.searchMitigationsRRF(ctx, q.Columns, q.DiffEmbedding)
	if err != nil {
		_ = c.recordAudit(ctx, "ELASTIC_LOOKUP_FAILED", q.ContractName, err)
		return nil, err
	}
	out := &ContractContext{
		YAMLDefinition:     yamlDef,
		MitigationPatterns: hits,
	}
	if len(hits) > 0 {
		out.RRFConfidence = hits[0].Score
	}
	if err := c.recordAudit(ctx, "ELASTIC_LOOKUP_OK", q.ContractName, nil); err != nil {
		return nil, fmt.Errorf("elastic: audit write failed: %w", err)
	}
	return out, nil
}

func (c *client) fetchContractYAML(ctx context.Context, name string) (string, error) {
	body, status, err := c.do(ctx, http.MethodGet, "/contracts/_doc/"+name, nil)
	if err != nil {
		return "", err
	}
	if status == http.StatusNotFound {
		return "", ErrContractNotFound
	}
	if status >= 400 {
		return "", &ElasticError{StatusCode: status, Body: string(body)}
	}
	var doc struct {
		Source struct {
			YAML string `json:"yaml"`
		} `json:"_source"`
		Found bool `json:"found"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return "", fmt.Errorf("elastic: decode contract: %w", err)
	}
	if !doc.Found {
		return "", ErrContractNotFound
	}
	return doc.Source.YAML, nil
}

func (c *client) searchMitigationsRRF(ctx context.Context, columns []string, embedding []float32) ([]Mitigation, error) {
	reqBody := map[string]any{
		"retriever": map[string]any{
			"rrf": map[string]any{
				"retrievers": []any{
					map[string]any{
						"standard": map[string]any{
							"query": map[string]any{
								"multi_match": map[string]any{
									"query":  strings.Join(columns, " "),
									"fields": []string{"columns", "table_metadata"},
								},
							},
						},
					},
					map[string]any{
						"knn": map[string]any{
							"field":          "diff_embedding",
							"query_vector":   embedding,
							"k":              10,
							"num_candidates": 100,
						},
					},
				},
				"rank_constant":    60,
				"rank_window_size": 100,
			},
		},
		"size": 3,
	}
	buf, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("elastic: marshal search: %w", err)
	}
	body, status, err := c.do(ctx, http.MethodPost, "/mitigations/_search", buf)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &ElasticError{StatusCode: status, Body: string(body)}
	}
	var res struct {
		Hits struct {
			Hits []struct {
				ID     string  `json:"_id"`
				Score  float64 `json:"_score"`
				Source struct {
					Summary string `json:"summary"`
					DiffID  string `json:"diff_id"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("elastic: decode search: %w", err)
	}
	out := make([]Mitigation, 0, len(res.Hits.Hits))
	for _, h := range res.Hits.Hits {
		id := h.Source.DiffID
		if id == "" {
			id = h.ID
		}
		out = append(out, Mitigation{DiffID: id, Score: h.Score, Summary: h.Source.Summary})
	}
	return out, nil
}

func (c *client) recordAudit(ctx context.Context, action, contract string, callErr error) error {
	payload := map[string]any{"contract": contract}
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
			return nil, 0, fmt.Errorf("elastic: build request: %w", err)
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
	return nil, 0, fmt.Errorf("%w: %v", ErrElasticUnavailable, lastErr)
}
