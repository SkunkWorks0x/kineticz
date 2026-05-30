package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOutboundTools(t *testing.T) {
	tools := OutboundTools()
	if len(tools) != 3 {
		t.Fatalf("len(OutboundTools) = %d, want 3", len(tools))
	}

	wantNames := map[string]bool{
		"elastic_lookup_contract":         false,
		"dynatrace_query_consumer_health": false,
		"arize_phoenix_query_traces":      false,
	}
	for _, td := range tools {
		if _, ok := wantNames[td.Name]; !ok {
			t.Errorf("unexpected tool name: %q", td.Name)
			continue
		}
		wantNames[td.Name] = true
		if td.Parameters.Type != "OBJECT" {
			t.Errorf("%s.Parameters.Type = %q, want OBJECT", td.Name, td.Parameters.Type)
		}
		if len(td.Parameters.Required) == 0 {
			t.Errorf("%s has no required parameters", td.Name)
		}
		// Must JSON-marshal cleanly (Gemini consumes JSON).
		if _, err := json.Marshal(td); err != nil {
			t.Errorf("%s: marshal failed: %v", td.Name, err)
		}
	}
	for name, seen := range wantNames {
		if !seen {
			t.Errorf("missing tool: %q", name)
		}
	}
}

func TestOutboundTools_ElasticHasRRFInDescription(t *testing.T) {
	for _, td := range OutboundTools() {
		if td.Name == "elastic_lookup_contract" {
			if !strings.Contains(td.Description, "Reciprocal Rank Fusion") {
				t.Errorf("elastic tool description missing RRF reference: %q", td.Description)
			}
			if !strings.Contains(td.Description, "60") {
				t.Errorf("elastic tool description missing rank_constant=60: %q", td.Description)
			}
			return
		}
	}
	t.Fatal("elastic_lookup_contract not found")
}

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
