package diagnose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/dynatrace"
	"github.com/skunkworks0x/kineticz/internal/elastic"
)

// DefaultTimeout caps the combined fan-out time for Elastic + Dynatrace.
const DefaultTimeout = 5 * time.Second

// DiagnosisResult is what Diagnose returns on success or graceful degradation.
// Degraded == true means Dynatrace failed softly; ConsumerHealth is nil and
// downstream stages should treat telemetry as unavailable for this run.
type DiagnosisResult struct {
	ContractContext  *elastic.ContractContext
	ConsumerHealth   []dynatrace.ConsumerHealth
	Degraded         bool
	CorrelationToken corr.CorrelationToken
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
}

// New wires an Engine with the default 5-second fan-out timeout.
func New(es elastic.Client, dt dynatrace.Client, aw audit.Writer) *Engine {
	return &Engine{
		elastic:   es,
		dynatrace: dt,
		audit:     aw,
		timeout:   DefaultTimeout,
	}
}

type esResult struct {
	cc  *elastic.ContractContext
	err error
}

type dtResult struct {
	ch  []dynatrace.ConsumerHealth
	err error
}

// Diagnose performs the parallel fan-out and applies the Partial Success
// Policy. Returns the result and DIAGNOSIS_OK / DIAGNOSIS_DEGRADED audit
// action on the happy paths; returns an error and DIAGNOSIS_FAILED on hard
// failures or timeout.
func (e *Engine) Diagnose(ctx context.Context, q elastic.ContractQuery, syncStartMs, syncEndMs int64) (*DiagnosisResult, error) {
	token, _ := corr.FromContext(ctx)

	timeoutCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	results := make(chan any, 2)
	go func() {
		cc, err := e.elastic.LookupContract(timeoutCtx, q)
		results <- esResult{cc: cc, err: err}
	}()
	go func() {
		ch, err := e.dynatrace.QueryConsumerHealth(timeoutCtx, syncStartMs, syncEndMs)
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
			_ = e.recordAudit(ctx, "DIAGNOSIS_FAILED", token, "timeout", timeoutCtx.Err().Error())
			return nil, timeoutCtx.Err()
		}
	}

	if es.err != nil {
		_ = e.recordAudit(ctx, "DIAGNOSIS_FAILED", token, "elastic", es.err.Error())
		return nil, fmt.Errorf("diagnose: elastic lookup: %w", es.err)
	}

	out := &DiagnosisResult{
		ContractContext:  es.cc,
		CorrelationToken: token,
	}

	if dt.err != nil {
		if errors.Is(dt.err, dynatrace.ErrTelemetryUnavailable) {
			out.Degraded = true
			_ = e.recordAudit(ctx, "DIAGNOSIS_DEGRADED", token, "dynatrace", dt.err.Error())
			return out, nil
		}
		_ = e.recordAudit(ctx, "DIAGNOSIS_FAILED", token, "dynatrace", dt.err.Error())
		return nil, fmt.Errorf("diagnose: dynatrace query: %w", dt.err)
	}

	out.ConsumerHealth = dt.ch
	_ = e.recordAudit(ctx, "DIAGNOSIS_OK", token, "", "")
	return out, nil
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
