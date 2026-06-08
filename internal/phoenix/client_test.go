package phoenix

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const repairSpansJSON = `{"spans":[
  {"name":"kineticz.repair","start_time":"2026-06-06T10:00:00Z",
   "attributes":{"kineticz.contract_name":"salesforce/orders","kineticz.final_verdict":"APPROVED","kineticz.iteration_count":2}},
  {"name":"kineticz.repair","start_time":"2026-06-05T09:00:00Z",
   "attributes":{"kineticz.contract_name":"postgres/users","kineticz.final_verdict":"MAX_ITERATIONS","kineticz.iteration_count":4}}
],"nextCursor":null}`

// fakeSession returns a client session wired to an in-memory server whose
// get-spans tool runs h. Used to drive the stdioClient without spawning node.
func fakeSession(t *testing.T, ctx context.Context, h mcp.ToolHandler) (*mcp.ClientSession, *mcp.ServerSession) {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "fake-phoenix", Version: "0.0.1"}, nil)
	srv.AddTool(&mcp.Tool{Name: "get-spans", InputSchema: map[string]any{"type": "object"}}, h)
	ct, st := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	c := mcp.NewClient(&mcp.Implementation{Name: "kineticz-test", Version: "0.0.1"}, nil)
	cs, err := c.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	return cs, ss
}

func textResult(s string) mcp.ToolHandler {
	return func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: s}}}, nil
	}
}

func TestQuerySpans_ParsesRepairSpans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, _ := fakeSession(t, ctx, textResult(repairSpansJSON))
	c := New(staticDialer(cs), "kineticz")
	defer c.Close()

	spans, err := c.QuerySpans(ctx, SpanQuery{Project: "kineticz", Names: []string{"kineticz.repair"}})
	if err != nil {
		t.Fatalf("QuerySpans: %v", err)
	}
	if len(spans) != 2 {
		t.Fatalf("want 2 spans, got %d", len(spans))
	}
	if spans[0].Name != "kineticz.repair" {
		t.Errorf("name = %q", spans[0].Name)
	}
	if got := spans[0].Attributes["kineticz.contract_name"]; got != "salesforce/orders" {
		t.Errorf("contract_name = %v", got)
	}
}

func TestQuerySpans_ReconnectsOnceOnDeadSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	attempts := 0
	dial := func(dctx context.Context) (*mcp.ClientSession, error) {
		attempts++
		if attempts == 1 {
			cs, ss := fakeSession(t, dctx, textResult(repairSpansJSON))
			ss.Close() // kill the server side so the first CallTool fails at the transport
			return cs, nil
		}
		cs, _ := fakeSession(t, dctx, textResult(repairSpansJSON))
		return cs, nil
	}
	c := New(dial, "kineticz")
	defer c.Close()

	spans, err := c.QuerySpans(ctx, SpanQuery{Project: "kineticz"})
	if err != nil {
		t.Fatalf("QuerySpans after reconnect: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("dial attempts = %d, want 2 (one reconnect)", attempts)
	}
	if len(spans) != 2 {
		t.Fatalf("want 2 spans after reconnect, got %d", len(spans))
	}
}

func TestQuerySpans_ToolErrorIsStructured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "project not found"}}}, nil
	}
	cs, _ := fakeSession(t, ctx, h)
	c := New(staticDialer(cs), "kineticz")
	defer c.Close()

	_, err := c.QuerySpans(ctx, SpanQuery{Project: "missing"})
	var pe *PhoenixError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *PhoenixError", err)
	}
}

func TestQuerySpans_BadJSONIsStructured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, _ := fakeSession(t, ctx, textResult("not json"))
	c := New(staticDialer(cs), "kineticz")
	defer c.Close()

	_, err := c.QuerySpans(ctx, SpanQuery{Project: "kineticz"})
	var pe *PhoenixError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *PhoenixError", err)
	}
}

// staticDialer returns the same session every dial. The reconnect test uses its
// own counting dialer instead.
func staticDialer(cs *mcp.ClientSession) Dialer {
	return func(context.Context) (*mcp.ClientSession, error) { return cs, nil }
}
