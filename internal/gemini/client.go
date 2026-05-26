package gemini

import (
	"context"
	"errors"
	"fmt"
)

// Client is the interface repair (and any future Gemini caller) depends on.
// Concrete implementation: vertexClient in vertex.go, backed by the Vertex AI
// REST API. Tests substitute Mock.
type Client interface {
	Generate(ctx context.Context, req GenerateRequest) (*Response, error)
}

// GenerateRequest is the per-call payload. The Vertex AI model and credentials
// live on the client itself; per-call config stays here.
type GenerateRequest struct {
	SystemInstruction string
	UserPrompt        string
	Temperature       float64
	MaxOutputTokens   int
}

// Response models the Vertex AI generateContent response shape. Each Part with
// Thought=true is a reasoning block; ExtractThought concatenates them.
type Response struct {
	Candidates []Candidate
	Usage      Usage
}

type Candidate struct {
	Content      Content
	FinishReason string
}

type Content struct {
	Parts []Part
}

type Part struct {
	Text    string
	Thought bool
}

type Usage struct {
	PromptTokenCount    int
	CandidateTokenCount int
	TotalTokenCount     int
}

// GeminiError carries a Vertex AI HTTP failure (4xx or 5xx after retries).
type GeminiError struct {
	StatusCode int
	Body       string
}

func (e *GeminiError) Error() string {
	return fmt.Sprintf("gemini: HTTP %d: %s", e.StatusCode, e.Body)
}

var (
	ErrGeminiUnavailable = errors.New("gemini: service unavailable")
	ErrInvalidResponse   = errors.New("gemini: invalid response shape")
)
