package diagnose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/skunkworks0x/kineticz/internal/arize"
	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/dynatrace"
	"github.com/skunkworks0x/kineticz/internal/elastic"
	"github.com/skunkworks0x/kineticz/internal/phoenix"
)

// DefaultTimeout caps the combined fan-out time for Elastic + Dynatrace.
const DefaultTimeout = 5 * time.Second

// DefaultPhoenixTimeout bounds the optional Phoenix self-introspection leg so a
// slow or dead MCP subprocess cannot extend the diagnose path.
const DefaultPhoenixTimeout = 2 * time.Second

// priorRepairLookback bounds how far back the Phoenix leg scans repair traces.
const priorRepairLookback = 7 * 24 * time.Hour

// DiagnosisResult is what Diagnose returns on success or graceful degradation.
// Degraded == true means Dynatrace failed softly; ConsumerHealth is nil and
// downstream stages should treat telemetry as unavailable for this run.
type DiagnosisResult struct {
	ContractContext  *elastic.ContractContext
	ConsumerHealth   []dynatrace.ConsumerHealth
	Degraded         bool
	CorrelationToken corr.CorrelationToken
	// ContractName identifies the contract under repair. Repair stamps it on its
	// span so a later diagnose can query Phoenix for this contract's history.
	ContractName string
	// PriorRepairs summarizes this contract's earlier repair attempts, read from
	// Phoenix. Empty when Phoenix is disabled, unreachable, or has no history.
	PriorRepairs []PriorRepair
}

// PriorRepair is one earlier repair attempt on the same contract, summarized
// from a kineticz.repair span.
type PriorRepair struct {
	Verdict    string
	Iterations int
	When       string
}

// Validation errors. DiagnosisResult crosses the engine boundary into repair
// and evaluate, so it gets the boundary-validation treatment from CLAUDE.md.
var (
	ErrNilContractContext             = errors.New("diagnose: ContractContext is required")
	ErrEmptyCorrelationToken          = errors.New("diagnose: CorrelationToken is empty")
	ErrMissingHealthInNonDegradedMode = errors.New("diagnose: ConsumerHealth required when not Degraded")
)

// Validate confirms r is well-formed before crossing the engine boundary into
// downstream stages. Returns a wrapped sentinel error per failure mode so
// callers can branch with errors.Is.
func (r *DiagnosisResult) Validate() error {
	if r == nil {
		return fmt.Errorf("diagnose: result is nil")
	}
	if r.ContractContext == nil {
		return ErrNilContractContext
	}
	if r.CorrelationToken == "" {
		return ErrEmptyCorrelationToken
	}
	if !r.Degraded && r.ConsumerHealth == nil {
		return ErrMissingHealthInNonDegradedMode
	}
	return nil
}

// Engine fans Elastic + Dynatrace out in parallel under a Partial Success
// Policy. Elastic failure is a hard fail (no diagnosis without a contract);
// Dynatrace ErrTelemetryUnavailable is a soft fail (continue with Degraded).
// Any other Dynatrace error (e.g., ErrCorrelationMissing) is a hard fail
// because it signals a caller bug, not a telemetry outage.
type Engine struct {
	elastic   elastic.Client
	dynatrace dynatrace.Client
	audit     audit.Writer
	timeout   time.Duration
	// Phoenix self-introspection leg. Optional: nil phoenix disables the leg
	// entirely, leaving the Elastic + Dynatrace path untouched.
	phoenix        phoenix.Client
	phoenixProject string
	phoenixTimeout time.Duration
}

// Option configures optional Engine legs.
type Option func(*Engine)

// WithPhoenix enables the self-introspection leg: the diagnose stage reads this
// contract's prior repair traces from Phoenix and attaches them to the result.
func WithPhoenix(c phoenix.Client, project string) Option {
	return func(e *Engine) {
		e.phoenix = c
		e.phoenixProject = project
	}
}

// New wires an Engine with the default 5-second fan-out timeout.
func New(es elastic.Client, dt dynatrace.Client, aw audit.Writer, opts ...Option) *Engine {
	e := &Engine{
		elastic:        es,
		dynatrace:      dt,
		audit:          aw,
		timeout:        DefaultTimeout,
		phoenixTimeout: DefaultPhoenixTimeout,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

type esResult struct {
	cc  *elastic.ContractContext
	err error
}

type dtResult struct {
	ch  []dynatrace.ConsumerHealth
	err error
}

type phoenixResult struct {
	priors []PriorRepair
	mode   string
}

// Diagnose performs the parallel fan-out and applies the Partial Success
// Policy. Returns the result and DIAGNOSIS_OK / DIAGNOSIS_DEGRADED audit
// action on the happy paths; returns an error and DIAGNOSIS_FAILED on hard
// failures or timeout.
func (e *Engine) Diagnose(ctx context.Context, q elastic.ContractQuery, syncStartMs, syncEndMs int64) (*DiagnosisResult, error) {
	token, _ := corr.FromContext(ctx)

	ctx, span := arize.Tracer().Start(ctx, "kineticz.diagnose")
	defer span.End()
	span.SetAttributes(
		attribute.String("openinference.span.kind", "CHAIN"),
		attribute.String("kineticz.contract_name", q.ContractName),
		attribute.String("kineticz.correlation_token", string(token)),
	)

	// The Phoenix leg runs off the parent ctx with its own short budget, so it
	// is independent of the Elastic + Dynatrace fan-out deadline and can never
	// trip the diagnose timeout. Its result is attached on the non-error returns.
	phoenixCh := e.launchPhoenix(ctx, q.ContractName)

	timeoutCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	results := make(chan any, 2)
	go func() {
		cctx, cspan := arize.Tracer().Start(timeoutCtx, "elastic.lookup_contract")
		defer cspan.End()
		cspan.SetAttributes(
			attribute.String("openinference.span.kind", "RETRIEVER"),
			attribute.String("kineticz.correlation_token", string(token)),
		)
		cc, err := e.elastic.LookupContract(cctx, q)
		if err != nil {
			cspan.SetStatus(codes.Error, err.Error())
			cspan.RecordError(err)
		} else if cc != nil && cc.Degraded {
			// Optional mitigations degraded (BM25 fallback or empty). Record it
			// on the span without marking ERROR; the repair pipeline continues.
			cspan.SetAttributes(
				attribute.Bool("retrieval.degraded", true),
				attribute.String("retrieval.mode", cc.MitigationsMode),
			)
			if cc.VectorErrorStatus != 0 {
				cspan.SetAttributes(attribute.Int("retrieval.vector_error_status", cc.VectorErrorStatus))
			}
			if cc.VectorErrorReason != "" {
				cspan.SetAttributes(attribute.String("retrieval.vector_error_reason", cc.VectorErrorReason))
			}
		}
		results <- esResult{cc: cc, err: err}
	}()
	go func() {
		cctx, cspan := arize.Tracer().Start(timeoutCtx, "dynatrace.query_consumer_health")
		defer cspan.End()
		cspan.SetAttributes(
			attribute.String("openinference.span.kind", "TOOL"),
			attribute.String("tool.name", "dynatrace.query_consumer_health"),
			attribute.String("kineticz.correlation_token", string(token)),
		)
		ch, err := e.dynatrace.QueryConsumerHealth(cctx, syncStartMs, syncEndMs)
		if err != nil {
			cspan.SetStatus(codes.Error, err.Error())
			cspan.RecordError(err)
		}
		results <- dtResult{ch: ch, err: err}
	}()

	var es esResult
	var dt dtResult
	for received := 0; received < 2; received++ {
		select {
		case r := <-results:
			switch v := r.(type) {
			case esResult:
				es = v
			case dtResult:
				dt = v
			}
		case <-timeoutCtx.Done():
			span.SetStatus(codes.Error, "timeout: "+timeoutCtx.Err().Error())
			span.RecordError(timeoutCtx.Err())
			_ = e.recordAudit(ctx, "DIAGNOSIS_FAILED", token, "timeout", timeoutCtx.Err().Error())
			return nil, timeoutCtx.Err()
		}
	}

	if es.err != nil {
		span.SetStatus(codes.Error, "elastic: "+es.err.Error())
		span.RecordError(es.err)
		_ = e.recordAudit(ctx, "DIAGNOSIS_FAILED", token, "elastic", es.err.Error())
		return nil, fmt.Errorf("diagnose: elastic lookup: %w", es.err)
	}

	out := &DiagnosisResult{
		ContractContext:  es.cc,
		CorrelationToken: token,
		ContractName:     q.ContractName,
	}

	// Record Elastic confidence regardless of Dynatrace outcome.
	if es.cc != nil {
		span.SetAttributes(attribute.Float64("kineticz.elastic_confidence_score", es.cc.RRFConfidence))
	}

	if dt.err != nil {
		var de *dynatrace.DynatraceError
		if errors.Is(dt.err, dynatrace.ErrTelemetryUnavailable) || errors.As(dt.err, &de) {
			out.Degraded = true
			// Transport failure -> UNAVAILABLE; an HTTP-response error (e.g. a
			// misconfigured Dynatrace URL returning 404) -> DEGRADED. Consumer
			// health is optional context, so the run continues either way.
			status := "UNAVAILABLE"
			if de != nil {
				status = "DEGRADED"
				span.SetAttributes(attribute.Int("kineticz.dynatrace_error_status", de.StatusCode))
			}
			span.SetAttributes(
				attribute.Bool("kineticz.degraded", true),
				attribute.String("kineticz.dynatrace_status", status),
			)
			e.attachPhoenix(ctx, span, out, phoenixCh, token)
			_ = e.recordAudit(ctx, "DIAGNOSIS_DEGRADED", token, "dynatrace", dt.err.Error())
			return out, nil
		}
		span.SetStatus(codes.Error, "dynatrace: "+dt.err.Error())
		span.RecordError(dt.err)
		span.SetAttributes(attribute.String("kineticz.dynatrace_status", "ERROR"))
		_ = e.recordAudit(ctx, "DIAGNOSIS_FAILED", token, "dynatrace", dt.err.Error())
		return nil, fmt.Errorf("diagnose: dynatrace query: %w", dt.err)
	}

	out.ConsumerHealth = dt.ch
	span.SetAttributes(
		attribute.Bool("kineticz.degraded", false),
		attribute.String("kineticz.dynatrace_status", "OK"),
	)
	e.attachPhoenix(ctx, span, out, phoenixCh, token)
	_ = e.recordAudit(ctx, "DIAGNOSIS_OK", token, "", "")
	return out, nil
}

// launchPhoenix starts the self-introspection leg, or returns nil when Phoenix
// is disabled. The watcher publishes on ch within the leg budget even when the
// query ignores its context (the MCP SDK can stall in session teardown). A
// stalled query that returns late drops its result into the buffered inner
// channel and the goroutine exits; a call that never returns leaks its
// goroutine, which the budget cannot reclaim. The "timeout" mode separates a
// budget overrun from a Phoenix error in the trace.
func (e *Engine) launchPhoenix(ctx context.Context, contract string) <-chan phoenixResult {
	if e.phoenix == nil {
		return nil
	}
	ch := make(chan phoenixResult, 1)
	pctx, pcancel := context.WithTimeout(ctx, e.phoenixTimeout)
	inner := make(chan phoenixResult, 1)
	go func() {
		priors, mode := e.queryPriorRepairs(pctx, contract)
		inner <- phoenixResult{priors: priors, mode: mode}
	}()
	go func() {
		defer pcancel()
		select {
		case r := <-inner:
			ch <- r
		case <-pctx.Done():
			ch <- phoenixResult{mode: "timeout"}
		}
	}()
	return ch
}

// queryPriorRepairs reads this contract's prior repair spans from Phoenix. It
// never returns an error: on any failure it reports the empty_optional mode and
// no priors, mirroring the Elastic mitigations fallback.
func (e *Engine) queryPriorRepairs(ctx context.Context, contract string) ([]PriorRepair, string) {
	token, _ := corr.FromContext(ctx)
	ctx, span := arize.Tracer().Start(ctx, "phoenix.query_prior_repairs")
	defer span.End()
	q := phoenix.SpanQuery{
		Project:            e.phoenixProject,
		Names:              []string{"kineticz.repair"},
		StartTime:          time.Now().Add(-priorRepairLookback).UTC().Format(time.RFC3339),
		IncludeAnnotations: true,
		Limit:              50,
	}
	in, _ := json.Marshal(q.Args())
	span.SetAttributes(
		attribute.String("openinference.span.kind", "TOOL"),
		attribute.String("tool.name", "phoenix.get_spans"),
		attribute.String("kineticz.contract_name", contract),
		attribute.String("kineticz.correlation_token", string(token)),
		attribute.String("input.value", string(in)),
	)
	spans, err := e.phoenix.QuerySpans(ctx, q)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		span.SetAttributes(attribute.String("retrieval.mode", "empty_optional"))
		return nil, "empty_optional"
	}
	priors := summarizePriorRepairs(spans, contract)
	out, _ := json.Marshal(priors)
	span.SetAttributes(
		attribute.String("retrieval.mode", "phoenix_history"),
		attribute.Int("kineticz.phoenix_prior_count", len(priors)),
		attribute.Int("kineticz.phoenix_spans_scanned", len(spans)),
		attribute.String("output.value", string(out)),
	)
	return priors, "phoenix_history"
}

// attachPhoenix waits for the leg's result (bounded by its own budget), records
// the mode on the span, and writes the PHOENIX_HISTORY_OK / _DEGRADED audit. A
// nil channel means the leg is disabled; nothing is attached or audited.
func (e *Engine) attachPhoenix(ctx context.Context, span trace.Span, out *DiagnosisResult, ch <-chan phoenixResult, token corr.CorrelationToken) {
	if ch == nil {
		return
	}
	var pr phoenixResult
	select {
	case pr = <-ch:
	case <-ctx.Done():
		pr = phoenixResult{mode: "empty_optional"}
	}
	out.PriorRepairs = pr.priors
	span.SetAttributes(
		attribute.String("kineticz.phoenix_mode", pr.mode),
		attribute.Int("kineticz.phoenix_prior_count", len(pr.priors)),
	)
	action := "PHOENIX_HISTORY_OK"
	if pr.mode != "phoenix_history" {
		action = "PHOENIX_HISTORY_DEGRADED"
	}
	_ = e.recordAudit(ctx, action, token, "phoenix", "")
}

// summarizePriorRepairs keeps the repair spans whose contract matches and folds
// each into a PriorRepair. Attribute values arrive as JSON scalars, so verdict
// and iteration count are coerced per key.
func summarizePriorRepairs(spans []phoenix.Span, contract string) []PriorRepair {
	var out []PriorRepair
	for _, s := range spans {
		if asString(s.Attributes["kineticz.contract_name"]) != contract {
			continue
		}
		out = append(out, PriorRepair{
			Verdict:    asString(s.Attributes["kineticz.final_verdict"]),
			Iterations: asInt(s.Attributes["kineticz.iteration_count"]),
			When:       s.StartTime,
		})
	}
	return out
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

func (e *Engine) recordAudit(ctx context.Context, action string, token corr.CorrelationToken, scope, errMsg string) error {
	payload := map[string]any{
		"correlation_token": string(token),
	}
	if scope != "" {
		payload["scope"] = scope
	}
	if errMsg != "" {
		payload["error"] = errMsg
	}
	body, _ := json.Marshal(payload)
	return e.audit.Append(ctx, action, body)
}
