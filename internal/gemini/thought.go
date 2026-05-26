package gemini

import "strings"

// ExtractThought concatenates the text of every Part flagged as a thinking
// block, joined by newlines. Designed to populate audit.Entry.Thought before
// audit.Chain so the reasoning becomes part of the signed hash.
//
// Duplicates mcp.ExtractThought intentionally; the mcp variant operates on
// mcp.GeminiResponse for the MCP tool layer's needs. Migration to a single
// implementation can happen once mcp's wire shape is finalized.
func ExtractThought(resp *Response) string {
	if resp == nil {
		return ""
	}
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
