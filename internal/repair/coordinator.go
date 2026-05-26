package repair

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/skunkworks0x/kineticz/internal/arize"
	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/engine/diagnose"
	"github.com/skunkworks0x/kineticz/internal/gemini"
)

const (
	DefaultMaxIterations = 4
	systemInstruction    = "You are a deterministic patch generator. Output a single-file unified diff in GitLab format. No prose outside the diff."
)

// Sentinel errors for repair outcomes.
var (
	ErrMaxIterationsExceeded = errors.New("repair: max iterations exceeded without approval")
	ErrTwoConsecutiveEmpty   = errors.New("repair: two consecutive empty Gemini responses")
)

// TargetReader fetches the latest content of the target file at the start of
// every iteration. The repair coordinator refreshes the buffer per iteration
// so concurrent edits to the source land in the next prompt.
type TargetReader interface {
	Read(ctx context.Context, path string) ([]byte, error)
}

// Result is the approved patch plus iteration metadata. Thought is the final
// iteration's reasoning block, also written into the REPAIR_APPROVED audit.
type Result struct {
	PatchDiff  []byte
	Iterations int
	Thought    string
}

// Coordinator runs up to MaxIterations of Gemini-generated patch attempts,
// validating each diff structurally before approving. Approval returns the
// raw diff bytes; downstream evaluate + commit stages consume them.
type Coordinator struct {
	gemini        gemini.Client
	audit         audit.ThoughtWriter
	target        TargetReader
	MaxIterations int
}

// New wires a Coordinator with the default 4 iterations.
func New(g gemini.Client, aw audit.ThoughtWriter, tr TargetReader) *Coordinator {
	return &Coordinator{
		gemini:        g,
		audit:         aw,
		target:        tr,
		MaxIterations: DefaultMaxIterations,
	}
}

// Repair runs the iteration loop against a validated DiagnosisResult. Each
// iteration: refresh target, prompt Gemini, parse diff, run structural
// checks, audit, retry if rejected. Returns the approved patch + metadata
// on success.
func (c *Coordinator) Repair(ctx context.Context, diag *diagnose.DiagnosisResult, targetPath string) (*Result, error) {
	if err := diag.Validate(); err != nil {
		return nil, fmt.Errorf("repair: invalid DiagnosisResult: %w", err)
	}

	ctx, span := arize.Tracer().Start(ctx, "kineticz.repair")
	defer span.End()
	if tok, ok := corr.FromContext(ctx); ok {
		span.SetAttributes(attribute.String("kineticz.correlation_token", string(tok)))
	}

	feedback := ""
	emptyCount := 0
	var lastThought string

	type iterationOutcome struct {
		approved bool
		result   *Result
		hardErr  error
	}

	for iter := 0; iter < c.MaxIterations; iter++ {
		outcome := func() iterationOutcome {
			iterCtx, iterSpan := arize.Tracer().Start(ctx, "kineticz.repair.iteration")
			iterSpan.SetAttributes(attribute.Int("kineticz.attempt_number", iter+1))
			defer iterSpan.End()

			target, err := c.target.Read(iterCtx, targetPath)
			if err != nil {
				iterSpan.SetStatus(codes.Error, err.Error())
				iterSpan.RecordError(err)
				c.writeAudit(iterCtx, "REPAIR_REJECTED", iter, "target_read_error", err.Error(), "")
				return iterationOutcome{hardErr: fmt.Errorf("repair: read target: %w", err)}
			}

			c.writeAudit(iterCtx, "REPAIR_ATTEMPT", iter, "", "", "")

			prompt := buildPrompt(diag, target, feedback)
			resp, err := c.gemini.Generate(iterCtx, gemini.GenerateRequest{
				SystemInstruction: systemInstruction,
				UserPrompt:        prompt,
				Temperature:       0.2,
				MaxOutputTokens:   4096,
			})
			if err != nil {
				iterSpan.SetStatus(codes.Error, "gemini: "+err.Error())
				iterSpan.RecordError(err)
				c.writeAudit(iterCtx, "REPAIR_REJECTED", iter, "gemini_error", err.Error(), "")
				emptyCount++
				if emptyCount >= 2 {
					return iterationOutcome{hardErr: fmt.Errorf("%w: %v", ErrTwoConsecutiveEmpty, err)}
				}
				feedback = "previous gemini call failed: " + err.Error()
				return iterationOutcome{}
			}

			thought := gemini.ExtractThought(resp)
			lastThought = thought
			diffBytes := []byte(extractDiffFromResponse(resp))

			files, _, parseErr := gitdiff.Parse(bytes.NewReader(diffBytes))
			if parseErr != nil {
				iterSpan.SetStatus(codes.Error, "parse: "+parseErr.Error())
				iterSpan.RecordError(parseErr)
				c.writeAudit(iterCtx, "REPAIR_REJECTED", iter, "parse_error", parseErr.Error(), thought)
				emptyCount++
				if emptyCount >= 2 {
					return iterationOutcome{hardErr: fmt.Errorf("%w: %v", ErrTwoConsecutiveEmpty, parseErr)}
				}
				feedback = "previous diff failed to parse: " + parseErr.Error()
				return iterationOutcome{}
			}

			if len(files) == 0 {
				iterSpan.SetAttributes(attribute.String("kineticz.iteration_verdict", "no_hunks"))
				c.writeAudit(iterCtx, "REPAIR_REJECTED", iter, "no_hunks", "", thought)
				emptyCount++
				if emptyCount >= 2 {
					return iterationOutcome{hardErr: ErrTwoConsecutiveEmpty}
				}
				feedback = "previous response contained no diff hunks"
				return iterationOutcome{}
			}

			emptyCount = 0
			reason := validateDiff(files)
			if reason == "" {
				iterSpan.SetAttributes(attribute.String("kineticz.iteration_verdict", "APPROVED"))
				c.writeAudit(iterCtx, "REPAIR_APPROVED", iter, "", "", thought)
				return iterationOutcome{
					approved: true,
					result: &Result{
						PatchDiff:  diffBytes,
						Iterations: iter + 1,
						Thought:    thought,
					},
				}
			}

			iterSpan.SetAttributes(attribute.String("kineticz.iteration_verdict", reason))
			c.writeAudit(iterCtx, "REPAIR_REJECTED", iter, reason, "", thought)
			feedback = "previous diff rejected: " + reason
			return iterationOutcome{}
		}()

		if outcome.hardErr != nil {
			span.SetStatus(codes.Error, outcome.hardErr.Error())
			span.RecordError(outcome.hardErr)
			span.SetAttributes(
				attribute.Int("kineticz.iteration_count", iter+1),
				attribute.String("kineticz.final_verdict", "HARD_FAIL"),
			)
			return nil, outcome.hardErr
		}
		if outcome.approved {
			span.SetAttributes(
				attribute.Int("kineticz.iteration_count", iter+1),
				attribute.String("kineticz.final_verdict", "APPROVED"),
			)
			return outcome.result, nil
		}
	}

	span.SetStatus(codes.Error, ErrMaxIterationsExceeded.Error())
	span.SetAttributes(
		attribute.Int("kineticz.iteration_count", c.MaxIterations),
		attribute.String("kineticz.final_verdict", "MAX_ITERATIONS"),
	)
	c.writeAudit(ctx, "REPAIR_REJECTED", c.MaxIterations-1, "max_iterations", "", lastThought)
	return nil, ErrMaxIterationsExceeded
}

// validateDiff returns "" if the parsed diff passes all structural checks, or
// the rejection reason string otherwise. Reasons are stable identifiers used
// in audit payloads and feedback strings.
func validateDiff(files []*gitdiff.File) string {
	if len(files) > 1 {
		return "multi_file"
	}
	f := files[0]
	if f.IsBinary {
		return "binary"
	}
	if strings.Contains(f.NewName, "..") || strings.HasPrefix(f.NewName, "/") {
		return "path_traversal"
	}
	added, removed := countAddRemove(f)
	if added == 0 && removed == 0 {
		return "empty_hunks"
	}
	return ""
}

func countAddRemove(f *gitdiff.File) (int, int) {
	var added, removed int
	for _, frag := range f.TextFragments {
		for _, line := range frag.Lines {
			switch line.Op {
			case gitdiff.OpAdd:
				added++
			case gitdiff.OpDelete:
				removed++
			}
		}
	}
	return added, removed
}

// extractDiffFromResponse concatenates non-thought parts and strips a
// surrounding markdown ```diff fence if present. Gemini commonly wraps code in
// fenced blocks; the gitdiff parser will not tolerate them.
func extractDiffFromResponse(resp *gemini.Response) string {
	if resp == nil {
		return ""
	}
	var b strings.Builder
	for _, c := range resp.Candidates {
		for _, p := range c.Content.Parts {
			if p.Thought {
				continue
			}
			b.WriteString(p.Text)
		}
	}
	text := b.String()
	if i := strings.Index(text, "```diff\n"); i >= 0 {
		rest := text[i+len("```diff\n"):]
		if j := strings.Index(rest, "```"); j >= 0 {
			return rest[:j]
		}
	}
	if i := strings.Index(text, "```\n"); i >= 0 {
		rest := text[i+len("```\n"):]
		if j := strings.Index(rest, "```"); j >= 0 {
			return rest[:j]
		}
	}
	return text
}

func buildPrompt(diag *diagnose.DiagnosisResult, target []byte, feedback string) string {
	var b strings.Builder
	b.WriteString("Contract definition:\n")
	b.WriteString(diag.ContractContext.YAMLDefinition)
	b.WriteString("\n\nTarget file contents:\n")
	b.Write(target)
	b.WriteString("\n\nTop historical mitigation patterns:\n")
	for i, m := range diag.ContractContext.MitigationPatterns {
		b.WriteString(fmt.Sprintf("  %d. %s (score=%.4f) — %s\n", i+1, m.DiffID, m.Score, m.Summary))
	}
	if diag.Degraded {
		b.WriteString("\nConsumer health: unavailable (degraded mode).\n")
	} else {
		b.WriteString("\nConsumer health (impacted services):\n")
		for _, h := range diag.ConsumerHealth {
			b.WriteString(fmt.Sprintf("  - %s: error_rate=%.4f latency_p95_ms=%.1f\n", h.Consumer, h.ErrorRate, h.LatencyP95Ms))
		}
	}
	if feedback != "" {
		b.WriteString("\nFeedback from previous attempt: ")
		b.WriteString(feedback)
		b.WriteString("\n")
	}
	b.WriteString("\nProduce a unified diff that resolves the upstream schema change. Single file only.")
	return b.String()
}

func (c *Coordinator) writeAudit(ctx context.Context, action string, iter int, reason, errMsg, thought string) {
	payload := map[string]any{"iteration": iter}
	if reason != "" {
		payload["reason"] = reason
	}
	if errMsg != "" {
		payload["error"] = errMsg
	}
	body, _ := json.Marshal(payload)
	if thought == "" {
		_ = c.audit.Append(ctx, action, body)
	} else {
		_ = c.audit.AppendWithThought(ctx, action, body, thought)
	}
}
