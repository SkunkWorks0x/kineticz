package evaluate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"

	"github.com/skunkworks0x/kineticz/internal/arize"
	"github.com/skunkworks0x/kineticz/internal/audit"
)

// RejectedIndexer indexes rejected diffs into Elastic so future retrieval
// can surface "we've seen this before, don't try it again" context. Failure
// is logged but doesn't propagate — degraded mode for the evaluate path.
type RejectedIndexer interface {
	Index(ctx context.Context, sha string, diff []byte) error
}

// Result describes which path Evaluate took and the outcome.
type Result struct {
	Verdict       Verdict
	LocalReason   string
	ArizeResponse *arize.EvaluateResponse
	DiffSHA       string
	Deduplicated  bool
}

// Gate orchestrates the two-layer evaluation: local pre-filter, then Arize.
// Rejected diffs are deduped within this Gate's lifetime; repeat submissions
// of the same SHA short-circuit to a cached BLOCK without re-running checks
// or re-indexing.
type Gate struct {
	arize   arize.Client
	audit   audit.Writer
	indexer RejectedIndexer
	dedup   sync.Map // sha256 hex -> struct{}
}

// New wires a Gate. arize is the boolean rubric service; aw writes audit
// entries to the MongoDB ledger; indexer indexes rejected diffs into Elastic.
func New(a arize.Client, aw audit.Writer, ri RejectedIndexer) *Gate {
	return &Gate{arize: a, audit: aw, indexer: ri}
}

// Evaluate runs the pipeline: dedup → local pre-filter → Arize gate.
// Audit writes always precede the async Elastic indexing to satisfy the
// "MongoDB before Elastic" invariant.
func (g *Gate) Evaluate(ctx context.Context, orig, patched, diff []byte) (*Result, error) {
	sha := diffSHA(diff)
	res := &Result{DiffSHA: sha}

	if _, dup := g.dedup.Load(sha); dup {
		res.Verdict = VerdictBlock
		res.Deduplicated = true
		return res, nil
	}

	local := RunLocal(orig, patched)
	if local.Verdict == VerdictBlock {
		// LoadOrStore is atomic: if a concurrent Evaluate raced past the
		// Load above and got here first, only one of us actually inserts
		// into the dedup map and writes the audit + index.
		if _, loaded := g.dedup.LoadOrStore(sha, struct{}{}); loaded {
			res.Verdict = VerdictBlock
			res.Deduplicated = true
			return res, nil
		}
		_ = g.audit.Append(ctx, "EVALUATE_LOCAL_BLOCK", payloadJSON(map[string]any{
			"sha":    sha,
			"reason": local.Reason,
		}))
		go g.indexRejected(ctx, sha, diff)
		res.Verdict = VerdictBlock
		res.LocalReason = local.Reason
		return res, nil
	}

	arizeResp, err := g.arize.Evaluate(ctx, arize.EvaluateRequest{
		Diff:    diff,
		Context: map[string]any{"diff_sha": sha},
	})
	if err != nil {
		if errors.Is(err, arize.ErrRubricFailed) {
			if _, loaded := g.dedup.LoadOrStore(sha, struct{}{}); loaded {
				res.Verdict = VerdictBlock
				res.Deduplicated = true
				return res, nil
			}
			_ = g.audit.Append(ctx, "EVALUATE_ARIZE_FAIL", payloadJSON(map[string]any{
				"sha":       sha,
				"rationale": arizeResp.Rationale,
			}))
			go g.indexRejected(ctx, sha, diff)
			res.Verdict = VerdictBlock
			res.ArizeResponse = arizeResp
			return res, nil
		}
		// Service unavailable or other transport error.
		_ = g.audit.Append(ctx, "EVALUATE_ARIZE_UNAVAILABLE", payloadJSON(map[string]any{
			"sha":   sha,
			"error": err.Error(),
		}))
		res.Verdict = VerdictBlock
		return res, err
	}

	_ = g.audit.Append(ctx, "EVALUATE_ARIZE_PASS", payloadJSON(map[string]any{
		"sha":       sha,
		"rationale": arizeResp.Rationale,
	}))
	res.Verdict = VerdictAllow
	res.ArizeResponse = arizeResp
	return res, nil
}

// indexRejected runs in a goroutine with a detached context so cancellation
// of the request ctx doesn't kill the indexing. Values (like the correlation
// token) still propagate via context.WithoutCancel.
func (g *Gate) indexRejected(parent context.Context, sha string, diff []byte) {
	ctx := context.WithoutCancel(parent)
	if err := g.indexer.Index(ctx, sha, diff); err != nil {
		// Best-effort: log nothing for now. A production logger would attach
		// the correlation token and report the failure for ops follow-up.
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
