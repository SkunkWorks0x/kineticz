package mcp

import "strings"

// GeminiResponse is the minimal shape Kineticz reads from a Gemini 3.5 Flash
// response. The full Vertex AI response carries more fields (usage, safety
// ratings, finish reasons) that ExtractThought ignores. Each Part flagged
// Thought=true contributes one line to the extracted reasoning.
type GeminiResponse struct {
	Candidates []GeminiCandidate `json:"candidates"`
}

type GeminiCandidate struct {
	Content GeminiContent `json:"content"`
}

type GeminiContent struct {
	Parts []GeminiPart `json:"parts"`
}

type GeminiPart struct {
	Text    string `json:"text,omitempty"`
	Thought bool   `json:"thought,omitempty"`
}

// ExtractThought concatenates the text of every Part flagged as a thinking
// block, joined by newlines. Returns empty string when no thinking parts are
// present. Callers populate audit.Entry.Thought with this before audit.Chain
// so the reasoning becomes part of the signed hash.
func ExtractThought(resp GeminiResponse) string {
	var parts []string
	for _, c := range resp.Candidates {
		for _, p := range c.Content.Parts {
			if p.Thought {
				parts = append(parts, p.Text)
			}
		}
	}
	return strings.Join(parts, "\n")
}
