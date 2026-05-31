package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/httputil"
)

type Client interface {
	LookupContract(ctx context.Context, q ContractQuery) (*ContractContext, error)
}

type ContractQuery struct {
	ContractName string
	Columns      []string
}

type ContractContext struct {
	YAMLDefinition     string
	MitigationPatterns []Mitigation
	RRFConfidence      float64
	// Mitigations retrieval is optional. These describe how it resolved.
	// MitigationsMode is one of "rrf", "bm25_fallback", "empty_optional";
	// Degraded is true for the latter two. VectorErrorReason is sanitized
	// (a class like no_ml_nodes/model_not_ready/429, or a <=200-char excerpt),
	// never the full Elastic error body.
	MitigationsMode   string
	Degraded          bool
	VectorErrorStatus int
	VectorErrorReason string
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
	http           *http.Client
	audit          audit.Writer
	baseURL        string
	apiKey         string
	inferenceModel string
	backoff        time.Duration
	retries        int
}

func NewClient(httpClient *http.Client, aw audit.Writer, baseURL, apiKey, inferenceModel string) *client {
	return &client{
		http:           httpClient,
		audit:          aw,
		baseURL:        baseURL,
		apiKey:         apiKey,
		inferenceModel: inferenceModel,
		backoff:        100 * time.Millisecond,
		retries:        3,
	}
}

func (c *client) LookupContract(ctx context.Context, q ContractQuery) (*ContractContext, error) {
	yamlDef, err := c.fetchContractYAML(ctx, q.ContractName)
	if err != nil {
		_ = c.recordAudit(ctx, "ELASTIC_LOOKUP_FAILED", q.ContractName, err)
		return nil, err
	}
	// Mitigations are optional: the vector (RRF) leg is opportunistic, BM25 is
	// the always-on fallback, and an empty result is acceptable. Only the
	// contract fetch above is fatal.
	hits, mode, vStatus, vReason := c.retrieveMitigations(ctx, q.ContractName)
	out := &ContractContext{
		YAMLDefinition:     yamlDef,
		MitigationPatterns: hits,
		MitigationsMode:    mode,
		Degraded:           mode != "rrf",
		VectorErrorStatus:  vStatus,
		VectorErrorReason:  vReason,
	}
	if len(hits) > 0 {
		out.RRFConfidence = hits[0].Score
	}
	if err := c.recordLookup(ctx, q.ContractName, mode, vStatus, vReason); err != nil {
		return nil, fmt.Errorf("elastic: audit write failed: %w", err)
	}
	return out, nil
}

func (c *client) fetchContractYAML(ctx context.Context, name string) (string, error) {
	// url.PathEscape so a slashed id (e.g. "postgres/orders_pg") stays one _id;
	// a raw slash makes ES read the path as _id="postgres" and 404 the lookup.
	body, status, err := c.do(ctx, http.MethodGet, "/contracts/_doc/"+url.PathEscape(name), nil)
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

func (c *client) searchMitigationsRRF(ctx context.Context, signature string) ([]Mitigation, error) {
	// Both RRF legs run on the connector signature (e.g. "postgres/sales_db").
	// BM25 matches it against columns/table_metadata; the KNN leg has Elastic
	// embed it with the E5 inference endpoint (query_vector_builder) and match
	// against the indexed diff_embedding vectors. Go sends no vector.
	reqBody := map[string]any{
		"retriever": map[string]any{
			"rrf": map[string]any{
				"retrievers": []any{
					map[string]any{
						"standard": map[string]any{
							"query": map[string]any{
								"multi_match": map[string]any{
									"query":  signature,
									"fields": []string{"columns", "table_metadata"},
								},
							},
						},
					},
					map[string]any{
						"knn": map[string]any{
							"field": "diff_embedding",
							"query_vector_builder": map[string]any{
								"text_embedding": map[string]any{
									"model_id":   c.inferenceModel,
									"model_text": signature,
								},
							},
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
	return c.searchMitigations(ctx, reqBody)
}

// searchMitigationsBM25 is the always-on fallback: the standard BM25 leg of the
// RRF query, with no KNN/query_vector_builder child, so it needs no inference
// and no ML node. RRF requires two child retrievers, so this drops the rrf
// wrapper and sends a plain multi_match.
func (c *client) searchMitigationsBM25(ctx context.Context, signature string) ([]Mitigation, error) {
	reqBody := map[string]any{
		"query": map[string]any{
			"multi_match": map[string]any{
				"query":  signature,
				"fields": []string{"columns", "table_metadata"},
			},
		},
		"size": 3,
	}
	return c.searchMitigations(ctx, reqBody)
}

// searchMitigations posts a mitigations _search body and parses the hits. The
// RRF and BM25-only callers share it.
func (c *client) searchMitigations(ctx context.Context, reqBody map[string]any) ([]Mitigation, error) {
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
	return parseMitigationHits(body)
}

func parseMitigationHits(body []byte) ([]Mitigation, error) {
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

// retrieveMitigations runs the RRF (vector) path opportunistically, retries once
// as BM25-only when the vector leg fails, then falls back to empty. Mitigations
// are optional, so this never returns an error. mode is "rrf", "bm25_fallback",
// or "empty_optional"; vStatus/vReason carry the vector leg's failure for
// telemetry when it fell back.
func (c *client) retrieveMitigations(ctx context.Context, signature string) (hits []Mitigation, mode string, vStatus int, vReason string) {
	hits, err := c.searchMitigationsRRF(ctx, signature)
	if err == nil {
		return hits, "rrf", 0, ""
	}
	vStatus, vReason = classifyVectorError(err)
	hits, err = c.searchMitigationsBM25(ctx, signature)
	if err == nil {
		return hits, "bm25_fallback", vStatus, vReason
	}
	return nil, "empty_optional", vStatus, vReason
}

// classifyVectorError turns the RRF failure into a status code and a short,
// non-sensitive reason. It never returns the full Elastic error body.
func classifyVectorError(err error) (status int, reason string) {
	var ee *ElasticError
	if errors.As(err, &ee) {
		status = ee.StatusCode
		b := strings.ToLower(ee.Body)
		switch {
		case strings.Contains(b, "no ml node"), strings.Contains(b, "no suitable nodes"):
			reason = "no_ml_nodes"
		case strings.Contains(b, "deployment"), strings.Contains(b, "not allocated"), strings.Contains(b, "not ready"):
			reason = "model_not_ready"
		case status == 429:
			reason = "429"
		default:
			reason = boundedReason(ee.Body)
		}
		return status, reason
	}
	if errors.Is(err, ErrElasticUnavailable) {
		return 0, "elastic_unavailable"
	}
	return 0, boundedReason(err.Error())
}

func boundedReason(s string) string {
	if len(s) > 200 {
		return s[:200]
	}
	return s
}

// recordLookup writes the lookup outcome to the audit ledger through the
// existing Append seam: ELASTIC_LOOKUP_OK on the full RRF path, or
// ELASTIC_LOOKUP_DEGRADED with the mode and sanitized vector status when the
// vector leg fell back. No change to the chain, signing, or canonical encoding.
func (c *client) recordLookup(ctx context.Context, contract, mode string, vStatus int, vReason string) error {
	action := "ELASTIC_LOOKUP_OK"
	payload := map[string]any{"contract": contract, "mode": mode}
	if mode != "rrf" {
		action = "ELASTIC_LOOKUP_DEGRADED"
		if vStatus != 0 {
			payload["vector_status"] = vStatus
		}
		if vReason != "" {
			payload["vector_reason"] = vReason
		}
	}
	body, _ := json.Marshal(payload)
	return c.audit.Append(ctx, action, body)
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
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, 0, fmt.Errorf("elastic: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// Secured cluster: API-key auth. c.apiKey is the base64 "id:api_key" value
	// Elasticsearch returns from POST /_security/api_key; pass it through as-is.
	req.Header.Set("Authorization", "ApiKey "+c.apiKey)
	resp, err := httputil.Do(ctx, c.http, req, c.retries, c.backoff)
	if err != nil {
		if errors.Is(err, httputil.ErrUnavailable) {
			return nil, 0, fmt.Errorf("%w: %v", ErrElasticUnavailable, err)
		}
		return nil, 0, err
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("elastic: read body: %w", err)
	}
	return buf, resp.StatusCode, nil
}
