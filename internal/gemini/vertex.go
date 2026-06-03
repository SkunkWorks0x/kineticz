package gemini

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/skunkworks0x/kineticz/internal/arize"
	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/httputil"
)

// VertexAccessTokenFunc returns a short-lived OAuth 2.0 access token for the
// Vertex AI API. Production callers wire this to Google Cloud's metadata
// server or a service-account credential refresh; tests can return a fixed
// string.
type VertexAccessTokenFunc func(ctx context.Context) (string, error)

type vertexClient struct {
	http      *http.Client
	audit     audit.Writer
	baseURL   string
	projectID string
	location  string
	model     string
	tokenFunc VertexAccessTokenFunc
	backoff   time.Duration
	retries   int
}

// NewVertexClient constructs a Client backed by the Vertex AI Generative AI
// REST endpoint. The model is fixed at construction (one Kineticz instance,
// one Gemini model). The default Vertex base URL is per-region; baseURL can be
// overridden for testing.
func NewVertexClient(httpClient *http.Client, aw audit.Writer, projectID, location, model string, tokenFunc VertexAccessTokenFunc) *vertexClient {
	// The "global" location has no region prefix: its host is the bare
	// aiplatform.googleapis.com (global-aiplatform.googleapis.com is not a valid
	// host). Regional locations keep the {location}- prefix. The request path
	// still carries locations/<location> in both cases (see Generate).
	host := fmt.Sprintf("https://%s-aiplatform.googleapis.com", location)
	if location == "global" {
		host = "https://aiplatform.googleapis.com"
	}
	return &vertexClient{
		http:      httpClient,
		audit:     aw,
		baseURL:   host,
		projectID: projectID,
		location:  location,
		model:     model,
		tokenFunc: tokenFunc,
		backoff:   200 * time.Millisecond,
		retries:   3,
	}
}

// vertexRequest mirrors the Vertex AI generateContent body shape.
type vertexRequest struct {
	Contents          []vertexContent `json:"contents"`
	SystemInstruction *vertexContent  `json:"systemInstruction,omitempty"`
	GenerationConfig  vertexGenConfig `json:"generationConfig"`
}

type vertexContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []vertexPart `json:"parts"`
}

type vertexPart struct {
	Text    string `json:"text,omitempty"`
	Thought bool   `json:"thought,omitempty"`
}

type vertexGenConfig struct {
	Temperature     float64               `json:"temperature,omitempty"`
	MaxOutputTokens int                   `json:"maxOutputTokens,omitempty"`
	ThinkingConfig  *vertexThinkingConfig `json:"thinkingConfig,omitempty"`
}

// vertexThinkingConfig opts the response into the thinking-block format.
// `[unverified]` against Vertex-specific REST shape; sourced from Google AI
// docs at https://ai.google.dev/gemini-api/docs/thinking. If Vertex rejects
// this field, the response simply omits thought parts and ExtractThought
// returns "" — the pipeline still runs, just without reasoning capture.
type vertexThinkingConfig struct {
	IncludeThoughts bool `json:"includeThoughts"`
}

type vertexResponse struct {
	Candidates []struct {
		Content struct {
			Parts []vertexPart `json:"parts"`
		} `json:"content"`
		FinishReason string `json:"finishReason"`
	} `json:"candidates"`
	UsageMetadata struct {
		PromptTokenCount     int `json:"promptTokenCount"`
		CandidatesTokenCount int `json:"candidatesTokenCount"`
		TotalTokenCount      int `json:"totalTokenCount"`
	} `json:"usageMetadata"`
}

func (c *vertexClient) Generate(ctx context.Context, req GenerateRequest) (*Response, error) {
	ctx, span := arize.Tracer().Start(ctx, "gemini.generate")
	defer span.End()
	span.SetAttributes(
		attribute.String("openinference.span.kind", "LLM"),
		attribute.String("llm.model_name", c.model),
		attribute.String("input.value", req.UserPrompt),
	)

	body, err := json.Marshal(buildVertexRequest(req))
	if err != nil {
		return nil, fmt.Errorf("gemini: marshal: %w", err)
	}

	token, err := c.tokenFunc(ctx)
	if err != nil {
		return nil, fmt.Errorf("gemini: fetch access token: %w", err)
	}

	url := fmt.Sprintf("%s/v1/projects/%s/locations/%s/publishers/google/models/%s:generateContent",
		c.baseURL, c.projectID, c.location, c.model)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		_ = c.recordAudit(ctx, "GEMINI_GENERATE_FAILED", err)
		return nil, fmt.Errorf("gemini: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)
	if tok, ok := corr.FromContext(ctx); ok {
		httpReq.Header.Set("X-Correlation-Token", string(tok))
	}

	resp, err := httputil.Do(ctx, c.http, httpReq, c.retries, c.backoff)
	if err != nil {
		if errors.Is(err, httputil.ErrUnavailable) {
			_ = c.recordAudit(ctx, "GEMINI_GENERATE_FAILED", err)
			return nil, fmt.Errorf("%w: %v", ErrGeminiUnavailable, err)
		}
		_ = c.recordAudit(ctx, "GEMINI_GENERATE_FAILED", err)
		return nil, err
	}
	defer resp.Body.Close()

	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		_ = c.recordAudit(ctx, "GEMINI_GENERATE_FAILED", err)
		return nil, fmt.Errorf("gemini: read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		e := &GeminiError{StatusCode: resp.StatusCode, Body: string(rb)}
		_ = c.recordAudit(ctx, "GEMINI_GENERATE_FAILED", e)
		return nil, e
	}

	var parsed vertexResponse
	if err := json.Unmarshal(rb, &parsed); err != nil {
		_ = c.recordAudit(ctx, "GEMINI_GENERATE_FAILED", err)
		return nil, fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	out := vertexResponseTo(&parsed)
	span.SetAttributes(attribute.String("output.value", string(rb)))
	if err := c.recordAudit(ctx, "GEMINI_GENERATE_OK", nil); err != nil {
		return nil, fmt.Errorf("gemini: audit write failed: %w", err)
	}
	return out, nil
}

func (c *vertexClient) recordAudit(ctx context.Context, action string, callErr error) error {
	payload := map[string]any{
		"model":    c.model,
		"location": c.location,
	}
	if callErr != nil {
		payload["error"] = callErr.Error()
	}
	body, _ := json.Marshal(payload)
	return c.audit.Append(ctx, action, body)
}

func buildVertexRequest(req GenerateRequest) vertexRequest {
	out := vertexRequest{
		Contents: []vertexContent{{
			Role:  "user",
			Parts: []vertexPart{{Text: req.UserPrompt}},
		}},
		GenerationConfig: vertexGenConfig{
			Temperature:     req.Temperature,
			MaxOutputTokens: req.MaxOutputTokens,
			ThinkingConfig:  &vertexThinkingConfig{IncludeThoughts: true},
		},
	}
	if req.SystemInstruction != "" {
		out.SystemInstruction = &vertexContent{
			Parts: []vertexPart{{Text: req.SystemInstruction}},
		}
	}
	return out
}

func vertexResponseTo(r *vertexResponse) *Response {
	out := &Response{
		Usage: Usage{
			PromptTokenCount:    r.UsageMetadata.PromptTokenCount,
			CandidateTokenCount: r.UsageMetadata.CandidatesTokenCount,
			TotalTokenCount:     r.UsageMetadata.TotalTokenCount,
		},
	}
	for _, c := range r.Candidates {
		parts := make([]Part, 0, len(c.Content.Parts))
		for _, p := range c.Content.Parts {
			parts = append(parts, Part{Text: p.Text, Thought: p.Thought})
		}
		out.Candidates = append(out.Candidates, Candidate{
			Content:      Content{Parts: parts},
			FinishReason: c.FinishReason,
		})
	}
	return out
}
