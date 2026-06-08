package phoenix

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestInMemoryTransport_FakesGetSpans proves the go-sdk in-memory transport can
// stand in for @arizeai/phoenix-mcp: a server exposing a get-spans tool that
// returns the {"spans":[...]} text-content shape phoenix-mcp emits, reachable
// by a client session that lists and calls it. This is the test seam the real
// client and diagnose-leg tests build on; if it could not fake get-spans, the
// Shape 2 build would have no offline test path.
func TestInMemoryTransport_FakesGetSpans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const spansJSON = `{"spans":[{"id":"span1","name":"kineticz.repair",` +
		`"attributes":{"kineticz.contract_name":"salesforce/orders",` +
		`"kineticz.final_verdict":"APPROVED"}}],"nextCursor":null}`

	srv := mcp.NewServer(&mcp.Implementation{Name: "fake-phoenix", Version: "0.0.1"}, nil)
	srv.AddTool(
		&mcp.Tool{Name: "get-spans", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: spansJSON}}}, nil
		},
	)

	clientT, serverT := mcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, serverT, nil); err != nil {
		t.Fatalf("server connect: %v", err)
	}

	client := mcp.NewClient(&mcp.Implementation{Name: "kineticz-test", Version: "0.0.1"}, nil)
	session, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer session.Close()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if len(tools.Tools) != 1 || tools.Tools[0].Name != "get-spans" {
		t.Fatalf("want one tool get-spans, got %+v", tools.Tools)
	}

	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get-spans",
		Arguments: map[string]any{"project_identifier": "kineticz", "names": []string{"kineticz.repair"}},
	})
	if err != nil {
		t.Fatalf("call get-spans: %v", err)
	}
	if res.IsError {
		t.Fatalf("call returned IsError, content=%+v", res.Content)
	}

	text := textContent(t, res)
	var parsed struct {
		Spans []struct {
			Name       string            `json:"name"`
			Attributes map[string]string `json:"attributes"`
		} `json:"spans"`
	}
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("unmarshal spans: %v\nraw: %s", err, text)
	}
	if len(parsed.Spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(parsed.Spans))
	}
	got := parsed.Spans[0].Attributes["kineticz.contract_name"]
	if got != "salesforce/orders" {
		t.Fatalf("contract_name: want salesforce/orders, got %q", got)
	}
}

func textContent(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			return tc.Text
		}
	}
	t.Fatalf("no text content in result: %+v", res.Content)
	return ""
}
