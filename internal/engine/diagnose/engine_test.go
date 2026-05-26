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

func TestDiagnose(t *testing.T) {
	contractCtx := &elastic.ContractContext{
		YAMLDefinition:     "name: users_v1\n",
		MitigationPatterns: []elastic.Mitigation{{DiffID: "diff-001", Score: 0.5}},
		RRFConfidence:      0.5,
	}
	health := []dynatrace.ConsumerHealth{
		{Consumer: "service-a", ErrorRate: 0.05, LatencyP95Ms: 120},
	}

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
