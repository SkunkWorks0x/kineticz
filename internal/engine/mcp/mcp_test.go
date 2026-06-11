package mcp

import "testing"

func TestInboundTools(t *testing.T) {
	tools := InboundTools()
	if len(tools) != 1 {
		t.Fatalf("len(InboundTools) = %d, want 1 (Diagnose only; Repair pending)", len(tools))
	}
	if tools[0].Name != "kineticz_diagnose" {
		t.Errorf("Name = %q, want kineticz_diagnose", tools[0].Name)
	}
	requiredHas := map[string]bool{
		"contract_name":     false,
		"columns":           false,
		"sync_start_ms":     false,
		"sync_end_ms":       false,
		"correlation_token": false,
	}
	for _, r := range tools[0].Parameters.Required {
		if _, ok := requiredHas[r]; ok {
			requiredHas[r] = true
		}
	}
	for name, seen := range requiredHas {
		if !seen {
			t.Errorf("Diagnose missing required field: %q", name)
		}
	}
}

func TestExtractThought(t *testing.T) {
	cases := []struct {
		name string
		resp GeminiResponse
		want string
	}{
		{
			name: "empty_response",
			resp: GeminiResponse{},
			want: "",
		},
		{
			name: "no_thought_parts",
			resp: GeminiResponse{
				Candidates: []GeminiCandidate{{
					Content: GeminiContent{
						Parts: []GeminiPart{
							{Text: "Here is the answer."},
						},
					},
				}},
			},
			want: "",
		},
		{
			name: "single_thought",
			resp: GeminiResponse{
				Candidates: []GeminiCandidate{{
					Content: GeminiContent{
						Parts: []GeminiPart{
							{Text: "Let me think.", Thought: true},
							{Text: "Final answer."},
						},
					},
				}},
			},
			want: "Let me think.",
		},
		{
			name: "multiple_thoughts_joined_by_newline",
			resp: GeminiResponse{
				Candidates: []GeminiCandidate{{
					Content: GeminiContent{
						Parts: []GeminiPart{
							{Text: "Step 1: analyze.", Thought: true},
							{Text: "Step 2: hypothesize.", Thought: true},
							{Text: "Answer.", Thought: false},
						},
					},
				}},
			},
			want: "Step 1: analyze.\nStep 2: hypothesize.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractThought(tc.resp)
			if got != tc.want {
				t.Errorf("ExtractThought = %q, want %q", got, tc.want)
			}
		})
	}
}
