# internal/phoenix

Runtime client for the Arize Phoenix MCP server. The diagnose stage uses it to
read Kineticz's own prior repair traces, so a new repair sees how earlier
attempts on the same contract resolved.

## MCP server

- Package: `@arizeai/phoenix-mcp` (npm), pinned at 4.0.13.
- Transport: stdio. The Go client spawns `node build/index.js` as a subprocess
  and speaks MCP over its stdin/stdout via `github.com/modelcontextprotocol/go-sdk`.
  There is no HTTP transport; the server exposes stdio only.
- Tool used: `get-spans`. The server exposes 27 tools across prompts, datasets,
  experiments, projects, traces, spans, sessions, and annotations.

## Configuration

The dialer passes these to the node subprocess:

| Env | Value |
| --- | --- |
| `PHOENIX_HOST` | Phoenix base URL. Cloud: `https://app.phoenix.arize.com/s/<space>`. This is the app URL with the space path, not the OTLP collector endpoint (`.../s/<space>/v1/traces`). |
| `PHOENIX_API_KEY` | Phoenix API key. Sent as a bearer token. |
| `PHOENIX_PROJECT` | Project the spans live in. Defaults to `default`. |

## Degradation

`QuerySpans` returns a `*PhoenixError` on connect, call, tool, or parse failure.
The diagnose leg treats any error as a soft failure: it records the degraded
mode and continues with no prior-repair context. A dead Phoenix never blocks the
apply path. The session is lazy and reconnects once if the node subprocess dies
between calls.
