package diagnose

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/dynatrace"
	"github.com/skunkworks0x/kineticz/internal/elastic"
	"github.com/skunkworks0x/kineticz/internal/phoenix"
)

type recordingAudit struct {
	mu      sync.Mutex
	actions []string
}

func (r *recordingAudit) Append(_ context.Context, action string, _ []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, action)
	return nil
}

func (r *recordingAudit) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.actions))
	copy(out, r.actions)
	return out
}

func (r *recordingAudit) has(action string) bool {
	for _, a := range r.snapshot() {
		if a == action {
			return true
		}
	}
	return false
}

func TestDiagnose(t *testing.T) {
	contractCtx := &elastic.ContractContext{
		YAMLDefinition:     "name: users_v1\n",
		MitigationPatterns: []elastic.Mitigation{{DiffID: "diff-001", Score: 0.5}},
		RRFConfidence:      0.5,
	}
	health := []dynatrace.ConsumerHealth{
		{Consumer: "service-a", ErrorRate: 0.05, LatencyP95Ms: 120},
	}
	// A raw (non-DynatraceError, non-sentinel) error must stay fatal: it is not
	// a telemetry outage. Used by the decode-style hard-fail case below.
	errDecode := errors.New("dynatrace: decode dql: unexpected end of JSON")

	cases := []struct {
		name         string
		esMock       *elastic.Mock
		dtMock       *dynatrace.Mock
		ctxToken     corr.CorrelationToken
		wantErrIs    error
		wantDegraded bool
		wantHealth   int
		wantAudits   []string
	}{
		{
			name: "both_succeed_returns_OK",
			esMock: &elastic.Mock{
				LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
					return contractCtx, nil
				},
			},
			dtMock: &dynatrace.Mock{
				QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
					return health, nil
				},
			},
			ctxToken:   "tok-happy",
			wantHealth: 1,
			wantAudits: []string{"DIAGNOSIS_OK"},
		},
		{
			name: "elastic_failure_is_hard_fail",
			esMock: &elastic.Mock{
				LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
					return nil, elastic.ErrContractNotFound
				},
			},
			dtMock: &dynatrace.Mock{
				QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
					return health, nil
				},
			},
			ctxToken:   "tok-es-fail",
			wantErrIs:  elastic.ErrContractNotFound,
			wantAudits: []string{"DIAGNOSIS_FAILED"},
		},
		{
			name: "dynatrace_telemetry_unavailable_is_soft_fail",
			esMock: &elastic.Mock{
				LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
					return contractCtx, nil
				},
			},
			dtMock: &dynatrace.Mock{
				QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
					return nil, dynatrace.ErrTelemetryUnavailable
				},
			},
			ctxToken:     "tok-dt-soft",
			wantDegraded: true,
			wantHealth:   0,
			wantAudits:   []string{"DIAGNOSIS_DEGRADED"},
		},
		{
			name: "dynatrace_http_404_degrades_soft",
			esMock: &elastic.Mock{
				LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
					return contractCtx, nil
				},
			},
			dtMock: &dynatrace.Mock{
				QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
					return nil, &dynatrace.DynatraceError{StatusCode: 404, Body: "404 page not found"}
				},
			},
			ctxToken:     "tok-dt-404",
			wantDegraded: true,
			wantHealth:   0,
			wantAudits:   []string{"DIAGNOSIS_DEGRADED"},
		},
		{
			name: "dynatrace_decode_error_is_hard_fail",
			esMock: &elastic.Mock{
				LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
					return contractCtx, nil
				},
			},
			dtMock: &dynatrace.Mock{
				QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
					return nil, errDecode
				},
			},
			ctxToken:   "tok-dt-decode",
			wantErrIs:  errDecode,
			wantAudits: []string{"DIAGNOSIS_FAILED"},
		},
		{
			name: "dynatrace_correlation_missing_is_hard_fail",
			esMock: &elastic.Mock{
				LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
					return contractCtx, nil
				},
			},
			dtMock: &dynatrace.Mock{
				QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
					return nil, dynatrace.ErrCorrelationMissing
				},
			},
			ctxToken:   "tok-dt-hard",
			wantErrIs:  dynatrace.ErrCorrelationMissing,
			wantAudits: []string{"DIAGNOSIS_FAILED"},
		},
		{
			name: "both_fail_returns_elastic_error",
			esMock: &elastic.Mock{
				LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
					return nil, elastic.ErrElasticUnavailable
				},
			},
			dtMock: &dynatrace.Mock{
				QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
					return nil, dynatrace.ErrTelemetryUnavailable
				},
			},
			ctxToken:   "tok-both-fail",
			wantErrIs:  elastic.ErrElasticUnavailable,
			wantAudits: []string{"DIAGNOSIS_FAILED"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			aw := &recordingAudit{}
			e := &Engine{
				elastic:   tc.esMock,
				dynatrace: tc.dtMock,
				audit:     aw,
				timeout:   2 * time.Second,
			}
			ctx := corr.WithToken(context.Background(), tc.ctxToken)
			q := elastic.ContractQuery{ContractName: "users_v1", Columns: []string{"id"}}

			res, err := e.Diagnose(ctx, q, 0, 1000)

			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
				if res != nil {
					t.Errorf("res = %+v, want nil on hard fail", res)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if res == nil {
					t.Fatal("res is nil")
				}
				if res.Degraded != tc.wantDegraded {
					t.Errorf("Degraded = %v, want %v", res.Degraded, tc.wantDegraded)
				}
				if len(res.ConsumerHealth) != tc.wantHealth {
					t.Errorf("len(ConsumerHealth) = %d, want %d", len(res.ConsumerHealth), tc.wantHealth)
				}
				if res.CorrelationToken != tc.ctxToken {
					t.Errorf("CorrelationToken = %q, want %q", res.CorrelationToken, tc.ctxToken)
				}
				if res.ContractContext == nil {
					t.Error("ContractContext is nil on success path")
				}
				if vErr := res.Validate(); vErr != nil {
					t.Errorf("Validate() = %v, want nil (degraded result must validate with nil health)", vErr)
				}
			}

			got := aw.snapshot()
			if len(got) != len(tc.wantAudits) {
				t.Fatalf("audits = %v, want %v", got, tc.wantAudits)
			}
			for i, want := range tc.wantAudits {
				if got[i] != want {
					t.Errorf("audit[%d] = %s, want %s", i, got[i], want)
				}
			}
		})
	}
}

func TestDiagnosisResult_Validate(t *testing.T) {
	cases := []struct {
		name      string
		r         *DiagnosisResult
		wantErrIs error
	}{
		{
			name: "valid_with_health",
			r: &DiagnosisResult{
				ContractContext:  &elastic.ContractContext{},
				ConsumerHealth:   []dynatrace.ConsumerHealth{},
				CorrelationToken: "tok",
			},
			wantErrIs: nil,
		},
		{
			name: "valid_degraded_nil_health_allowed",
			r: &DiagnosisResult{
				ContractContext:  &elastic.ContractContext{},
				Degraded:         true,
				CorrelationToken: "tok",
			},
			wantErrIs: nil,
		},
		{
			name:      "nil_ContractContext",
			r:         &DiagnosisResult{CorrelationToken: "tok", ConsumerHealth: []dynatrace.ConsumerHealth{}},
			wantErrIs: ErrNilContractContext,
		},
		{
			name: "empty_CorrelationToken",
			r: &DiagnosisResult{
				ContractContext: &elastic.ContractContext{},
				ConsumerHealth:  []dynatrace.ConsumerHealth{},
			},
			wantErrIs: ErrEmptyCorrelationToken,
		},
		{
			name: "non_degraded_nil_ConsumerHealth_rejected",
			r: &DiagnosisResult{
				ContractContext:  &elastic.ContractContext{},
				CorrelationToken: "tok",
			},
			wantErrIs: ErrMissingHealthInNonDegradedMode,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.r.Validate()
			if tc.wantErrIs == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErrIs)
			}
		})
	}
}

func TestDiagnose_Timeout(t *testing.T) {
	slowEs := &elastic.Mock{
		LookupContractFn: func(ctx context.Context, _ elastic.ContractQuery) (*elastic.ContractContext, error) {
			select {
			case <-time.After(200 * time.Millisecond):
				return &elastic.ContractContext{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	slowDt := &dynatrace.Mock{
		QueryConsumerHealthFn: func(ctx context.Context, _, _ int64) ([]dynatrace.ConsumerHealth, error) {
			select {
			case <-time.After(200 * time.Millisecond):
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	aw := &recordingAudit{}
	e := &Engine{
		elastic:   slowEs,
		dynatrace: slowDt,
		audit:     aw,
		timeout:   10 * time.Millisecond,
	}
	ctx := corr.WithToken(context.Background(), "tok-timeout")
	_, err := e.Diagnose(ctx, elastic.ContractQuery{}, 0, 1000)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	got := aw.snapshot()
	if len(got) == 0 || got[0] != "DIAGNOSIS_FAILED" {
		t.Fatalf("audits = %v, want first DIAGNOSIS_FAILED", got)
	}
}

func okMocks(cc *elastic.ContractContext, health []dynatrace.ConsumerHealth) (*elastic.Mock, *dynatrace.Mock) {
	return &elastic.Mock{
			LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
				return cc, nil
			},
		}, &dynatrace.Mock{
			QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
				return health, nil
			},
		}
}

func TestDiagnose_PhoenixPriorRepairs(t *testing.T) {
	es, dt := okMocks(
		&elastic.ContractContext{YAMLDefinition: "name: orders\n"},
		[]dynatrace.ConsumerHealth{{Consumer: "svc", ErrorRate: 0.01, LatencyP95Ms: 100}},
	)
	pmock := &phoenix.Mock{
		QuerySpansFn: func(_ context.Context, q phoenix.SpanQuery) ([]phoenix.Span, error) {
			return []phoenix.Span{
				{Name: "kineticz.repair", StartTime: "2026-06-06T10:00:00Z", Attributes: map[string]any{
					"kineticz.contract_name": "salesforce/orders", "kineticz.final_verdict": "MAX_ITERATIONS", "kineticz.iteration_count": 4}},
				{Name: "kineticz.repair", StartTime: "2026-06-05T09:00:00Z", Attributes: map[string]any{
					"kineticz.contract_name": "postgres/users", "kineticz.final_verdict": "APPROVED", "kineticz.iteration_count": 1}},
			}, nil
		},
	}
	aw := &recordingAudit{}
	e := &Engine{
		elastic: es, dynatrace: dt, audit: aw, timeout: 2 * time.Second,
		phoenix: pmock, phoenixProject: "default", phoenixTimeout: time.Second,
	}
	ctx := corr.WithToken(context.Background(), "tok-phoenix")
	q := elastic.ContractQuery{ContractName: "salesforce/orders", Columns: []string{"id"}}

	res, err := e.Diagnose(ctx, q, 0, 1000)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(res.PriorRepairs) != 1 {
		t.Fatalf("PriorRepairs = %+v, want 1 (filtered by contract_name)", res.PriorRepairs)
	}
	if res.PriorRepairs[0].Verdict != "MAX_ITERATIONS" || res.PriorRepairs[0].Iterations != 4 {
		t.Errorf("prior = %+v, want verdict=MAX_ITERATIONS iterations=4", res.PriorRepairs[0])
	}
	if res.ContractName != "salesforce/orders" {
		t.Errorf("ContractName = %q", res.ContractName)
	}
	if !aw.has("PHOENIX_HISTORY_OK") {
		t.Errorf("audits = %v, want PHOENIX_HISTORY_OK", aw.snapshot())
	}
	if !aw.has("DIAGNOSIS_OK") {
		t.Errorf("audits = %v, want DIAGNOSIS_OK", aw.snapshot())
	}
}

func TestDiagnose_PhoenixDegradesSoftly(t *testing.T) {
	es, dt := okMocks(
		&elastic.ContractContext{YAMLDefinition: "name: orders\n"},
		[]dynatrace.ConsumerHealth{{Consumer: "svc", ErrorRate: 0.01, LatencyP95Ms: 100}},
	)
	pmock := &phoenix.Mock{
		QuerySpansFn: func(context.Context, phoenix.SpanQuery) ([]phoenix.Span, error) {
			return nil, &phoenix.PhoenixError{Op: "connect", Err: errors.New("npx spawn failed")}
		},
	}
	aw := &recordingAudit{}
	e := &Engine{
		elastic: es, dynatrace: dt, audit: aw, timeout: 2 * time.Second,
		phoenix: pmock, phoenixProject: "default", phoenixTimeout: time.Second,
	}
	ctx := corr.WithToken(context.Background(), "tok-phoenix-dead")
	q := elastic.ContractQuery{ContractName: "salesforce/orders"}

	res, err := e.Diagnose(ctx, q, 0, 1000)
	if err != nil {
		t.Fatalf("dead Phoenix must not error diagnose: %v", err)
	}
	if len(res.PriorRepairs) != 0 {
		t.Errorf("PriorRepairs = %+v, want empty on degrade", res.PriorRepairs)
	}
	if res.ContractContext == nil {
		t.Error("apply path broken: ContractContext nil")
	}
	if !aw.has("PHOENIX_HISTORY_DEGRADED") {
		t.Errorf("audits = %v, want PHOENIX_HISTORY_DEGRADED", aw.snapshot())
	}
	if !aw.has("DIAGNOSIS_OK") {
		t.Errorf("audits = %v, want DIAGNOSIS_OK (apply path unaffected)", aw.snapshot())
	}
}
