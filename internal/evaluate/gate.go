package evaluate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/skunkworks0x/kineticz/internal/arize"
	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
)

// RejectedIndexer indexes rejected diffs into Elastic so future retrieval
// can surface "we've seen this before, don't try it again" context. Failure
// is logged but doesn't propagate.
type RejectedIndexer interface {
	Index(ctx context.Context, sha string, diff []byte) error
}

// Result describes the outcome of Evaluate.
type Result struct {
	Verdict      Verdict
	LocalReason  string
	DiffSHA      string
	Deduplicated bool
}

// Gate runs the deterministic local pre-filter. The Arize boolean rubric is
// now implemented by these local checks (parses-as-Go + preserves-exported-
// signatures); Arize Phoenix observes the decision via OpenTelemetry spans
// rather than scoring it via a REST endpoint. Rejected diffs are deduped
// within this Gate's lifetime.
type Gate struct {
	audit   audit.Writer
	indexer RejectedIndexer
	dedup   sync.Map // sha256 hex -> struct{}
}

// New wires a Gate. aw writes audit entries to the MongoDB ledger; indexer
// indexes rejected diffs into Elastic.
func New(aw audit.Writer, ri RejectedIndexer) *Gate {
	return &Gate{audit: aw, indexer: ri}
}

// Evaluate runs the pipeline: dedup → local pre-filter → audit + trace.
// Audit writes always precede the async Elastic indexing to satisfy the
// "MongoDB before Elastic" invariant. A span is emitted for Phoenix.
func (g *Gate) Evaluate(ctx context.Context, orig, patched, diff []byte) (*Result, error) {
	sha := diffSHA(diff)
	res := &Result{DiffSHA: sha}

	ctx, span := arize.Tracer().Start(ctx, "kineticz.evaluate")
	span.SetAttributes(attribute.String("diff.sha256", sha))
	if tok, ok := corr.FromContext(ctx); ok {
		span.SetAttributes(attribute.String("kineticz.correlation_token", string(tok)))
	}
	defer span.End()

	if _, dup := g.dedup.Load(sha); dup {
		span.SetAttributes(attribute.Bool("evaluate.deduplicated", true))
		res.Verdict = VerdictBlock
		res.Deduplicated = true
		return res, nil
	}

	local := RunLocal(orig, patched)
	span.SetAttributes(
		attribute.Bool("kineticz.parses_as_go", local.ParsesAsGo),
		attribute.Bool("kineticz.signature_preserved", local.SignaturePreserved),
	)
	if local.Verdict == VerdictBlock {
		if _, loaded := g.dedup.LoadOrStore(sha, struct{}{}); loaded {
			res.Verdict = VerdictBlock
			res.Deduplicated = true
			return res, nil
		}
		span.SetStatus(codes.Error, local.Reason)
		span.SetAttributes(attribute.String("evaluate.verdict", "BLOCK"), attribute.String("evaluate.reason", local.Reason))
		_ = g.audit.Append(ctx, "EVALUATE_LOCAL_BLOCK", payloadJSON(map[string]any{
			"sha":    sha,
			"reason": local.Reason,
		}))
		go g.indexRejected(ctx, sha, diff)
		res.Verdict = VerdictBlock
		res.LocalReason = local.Reason
		return res, nil
	}

	span.SetAttributes(attribute.String("evaluate.verdict", "PASS"))
	_ = g.audit.Append(ctx, "EVALUATE_PASS", payloadJSON(map[string]any{"sha": sha}))
	res.Verdict = VerdictAllow
	return res, nil
}

// indexRejected runs in a goroutine with a detached context so cancellation
// of the request ctx doesn't kill the indexing. Values (like the correlation
// token) still propagate via context.WithoutCancel.
func (g *Gate) indexRejected(parent context.Context, sha string, diff []byte) {
	ctx := context.WithoutCancel(parent)
	if err := g.indexer.Index(ctx, sha, diff); err != nil {
		_ = err
	}
}

func diffSHA(diff []byte) string {
	sum := sha256.Sum256(diff)
	return hex.EncodeToString(sum[:])
}

func payloadJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
