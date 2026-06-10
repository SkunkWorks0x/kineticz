// Package phoenix is the runtime client for the Arize Phoenix MCP server
// (@arizeai/phoenix-mcp). It speaks MCP over stdio to a node subprocess and
// exposes get-spans so the diagnose stage can read Kineticz's own prior repair
// traces. The MCP server URL and transport are documented in README.md.
package phoenix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Client reads spans from Phoenix. The diagnose leg treats any error as a soft
// failure, so callers degrade rather than propagate.
type Client interface {
	QuerySpans(ctx context.Context, q SpanQuery) ([]Span, error)
	Close() error
}

// SpanQuery maps to the phoenix-mcp get-spans tool arguments. Zero-valued
// fields are omitted from the call.
type SpanQuery struct {
	Project            string
	Names              []string
	StartTime          string // RFC3339; get-spans start_time (inclusive lower bound)
	Limit              int
	IncludeAnnotations bool
}

// Span is the subset of a Phoenix span the diagnose stage reads. Attributes
// keeps raw JSON values (strings, numbers) so the caller coerces per key.
type Span struct {
	Name       string         `json:"name"`
	StartTime  string         `json:"start_time"`
	Attributes map[string]any `json:"attributes"`
}

// PhoenixError wraps a failure at one stage of a get-spans call. Op is one of
// connect, call, tool, parse.
type PhoenixError struct {
	Op  string
	Err error
}

func (e *PhoenixError) Error() string { return fmt.Sprintf("phoenix: %s: %v", e.Op, e.Err) }
func (e *PhoenixError) Unwrap() error { return e.Err }

// Dialer opens a fresh MCP client session. Production spawns the node
// subprocess (NodeDialer); tests inject an in-memory transport. This is the
// stdio analog of the *http.Client seam other Kineticz clients accept.
type Dialer func(ctx context.Context) (*mcp.ClientSession, error)

type stdioClient struct {
	dial    Dialer
	project string

	mu      sync.Mutex
	session *mcp.ClientSession
}

// New builds a Client. The session is lazy: the first QuerySpans dials, and the
// session is reused across calls until one fails, which triggers a single
// reconnect.
func New(dial Dialer, project string) *stdioClient {
	return &stdioClient{dial: dial, project: project}
}

func (c *stdioClient) QuerySpans(ctx context.Context, q SpanQuery) ([]Span, error) {
	if q.Project == "" {
		q.Project = c.project
	}
	spans, err := c.callOnce(ctx, q)
	if err == nil {
		return spans, nil
	}
	// Reconnect once, and only when the transport failed: the long-lived node
	// session may have died between calls. Tool, parse, and JSON-RPC protocol
	// failures (unknown tool, schema-invalid arguments) are deterministic, so
	// a redial repeats the same failure at double the subprocess cost. A
	// failed dial already had a fresh subprocess, and an expired context dooms
	// the second dial before it starts.
	var pe *PhoenixError
	var je *jsonrpc.Error
	if !errors.As(err, &pe) || pe.Op != "call" || errors.As(err, &je) || ctx.Err() != nil {
		return nil, err
	}
	c.reset()
	return c.callOnce(ctx, q)
}

func (c *stdioClient) callOnce(ctx context.Context, q SpanQuery) ([]Span, error) {
	session, err := c.ensure(ctx)
	if err != nil {
		return nil, &PhoenixError{Op: "connect", Err: err}
	}
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "get-spans", Arguments: q.Args()})
	if err != nil {
		return nil, &PhoenixError{Op: "call", Err: err}
	}
	if res.IsError {
		return nil, &PhoenixError{Op: "tool", Err: fmt.Errorf("get-spans error: %s", firstText(res))}
	}
	return parseSpans(res)
}

func (c *stdioClient) ensure(ctx context.Context) (*mcp.ClientSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		return c.session, nil
	}
	session, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	c.session = session
	return session, nil
}

func (c *stdioClient) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		_ = c.session.Close()
		c.session = nil
	}
}

func (c *stdioClient) Close() error {
	c.reset()
	return nil
}

// Args returns the get-spans tool arguments for this query. The diagnose stage
// stamps the same map on its introspection span as input.value.
func (q SpanQuery) Args() map[string]any {
	args := map[string]any{"project_identifier": q.Project}
	if len(q.Names) > 0 {
		args["names"] = q.Names
	}
	if q.StartTime != "" {
		args["start_time"] = q.StartTime
	}
	if q.Limit > 0 {
		args["limit"] = q.Limit
	}
	if q.IncludeAnnotations {
		args["include_annotations"] = true
	}
	return args
}

func parseSpans(res *mcp.CallToolResult) ([]Span, error) {
	var body struct {
		Spans []Span `json:"spans"`
	}
	if err := json.Unmarshal([]byte(firstText(res)), &body); err != nil {
		return nil, &PhoenixError{Op: "parse", Err: err}
	}
	return body.Spans, nil
}

func firstText(res *mcp.CallToolResult) string {
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			return tc.Text
		}
	}
	return ""
}

// NodeDialer spawns the phoenix-mcp node process and connects over its stdio.
// nodePath and entryPath are the node binary and the phoenix-mcp build/index.js,
// baked into the image (see Dockerfile). env supplies PHOENIX_HOST,
// PHOENIX_API_KEY, PHOENIX_PROJECT.
func NodeDialer(nodePath, entryPath string, env []string) Dialer {
	return func(ctx context.Context) (*mcp.ClientSession, error) {
		cmd := exec.Command(nodePath, entryPath)
		cmd.Env = env
		cmd.Stderr = os.Stderr
		client := mcp.NewClient(&mcp.Implementation{Name: "kineticz", Version: "0.1.0"}, nil)
		return client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	}
}
