package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/skunkworks0x/kineticz/internal/corr"
)

type recordingAudit struct {
	mu      sync.Mutex
	entries []recordedAuditEntry
}

type recordedAuditEntry struct {
	Action  string
	Payload []byte
	Token   corr.CorrelationToken
}

func (r *recordingAudit) Append(ctx context.Context, action string, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	tok, _ := corr.FromContext(ctx)
	r.entries = append(r.entries, recordedAuditEntry{
		Action:  action,
		Payload: append([]byte(nil), payload...),
		Token:   tok,
	})
	return nil
}

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	p := filepath.Join("testdata", name)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return b
}

func TestLookupContract(t *testing.T) {
	contractFix := loadFixture(t, "contract.json")
	searchFix := loadFixture(t, "rrf_search.json")

	cases := []struct {
		name           string
		handler        http.HandlerFunc
		token          corr.CorrelationToken
		wantErrIs      error
		wantAuditOps   []string
		wantConfidence float64
		wantTopID      string
	}{
		{
			name: "happy_path_returns_yaml_and_top_mitigations",
			handler: func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/contracts/_doc/"):
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(contractFix)
				case r.Method == http.MethodPost && r.URL.Path == "/mitigations/_search":
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						http.Error(w, "decode error", http.StatusBadRequest)
						return
					}
					retriever, _ := body["retriever"].(map[string]any)
					rrf, ok := retriever["rrf"].(map[string]any)
					if !ok {
						http.Error(w, "missing retriever.rrf", http.StatusBadRequest)
						return
					}
					if rc, _ := rrf["rank_constant"].(float64); rc != 60 {
						http.Error(w, "rank_constant must be 60", http.StatusBadRequest)
						return
					}
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(searchFix)
				default:
					http.Error(w, "unexpected path", http.StatusNotFound)
				}
			},
			token:          "tok-happy",
			wantAuditOps:   []string{"ELASTIC_LOOKUP_OK"},
			wantConfidence: 0.032520326,
			wantTopID:      "diff-001",
		},
		{
			name: "contract_404_returns_ErrContractNotFound",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"_index":"contracts","_id":"x","found":false}`))
			},
			token:        "tok-404",
			wantErrIs:    ErrContractNotFound,
			wantAuditOps: []string{"ELASTIC_LOOKUP_FAILED"},
		},
		{
			name: "5xx_exhausts_retries_and_returns_ErrElasticUnavailable",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"cluster unavailable"}`))
			},
			token:        "tok-5xx",
			wantErrIs:    ErrElasticUnavailable,
			wantAuditOps: []string{"ELASTIC_LOOKUP_FAILED"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			aw := &recordingAudit{}
			c := &client{
				http:    server.Client(),
				audit:   aw,
				baseURL: server.URL,
				backoff: 1 * time.Millisecond,
				retries: 3,
			}

			ctx := corr.WithToken(context.Background(), tc.token)
			q := ContractQuery{
				ContractName: "users_v1",
				Columns:      []string{"id", "email", "created_at"},
			}

			cc, err := c.LookupContract(ctx, q)

			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if cc == nil {
					t.Fatal("ContractContext is nil")
				}
				if cc.RRFConfidence != tc.wantConfidence {
					t.Errorf("RRFConfidence = %v, want %v", cc.RRFConfidence, tc.wantConfidence)
				}
				if len(cc.MitigationPatterns) != 3 {
					t.Errorf("want 3 mitigations, got %d", len(cc.MitigationPatterns))
				}
				if cc.MitigationPatterns[0].DiffID != tc.wantTopID {
					t.Errorf("top DiffID = %q, want %q", cc.MitigationPatterns[0].DiffID, tc.wantTopID)
				}
				if !strings.Contains(cc.YAMLDefinition, "name: users_v1") {
					t.Errorf("YAML missing expected content: %s", cc.YAMLDefinition)
				}
			}

			aw.mu.Lock()
			defer aw.mu.Unlock()
			if len(aw.entries) != len(tc.wantAuditOps) {
				t.Fatalf("audit count = %d, want %d (%v)", len(aw.entries), len(tc.wantAuditOps), tc.wantAuditOps)
			}
			for i, want := range tc.wantAuditOps {
				if aw.entries[i].Action != want {
					t.Errorf("audit[%d].Action = %s, want %s", i, aw.entries[i].Action, want)
				}
				if aw.entries[i].Token != tc.token {
					t.Errorf("audit[%d].Token = %q, want %q", i, aw.entries[i].Token, tc.token)
				}
			}
		})
	}
}

func TestLookupContract_ContractNameWithSlashIsPathEscaped(t *testing.T) {
	contractFix := loadFixture(t, "contract.json")
	searchFix := loadFixture(t, "rrf_search.json")

	var mu sync.Mutex
	var gotContractTarget string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/contracts/_doc/"):
			mu.Lock()
			gotContractTarget = r.RequestURI
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(contractFix)
		case r.Method == http.MethodPost && r.URL.Path == "/mitigations/_search":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(searchFix)
		default:
			http.Error(w, "unexpected target: "+r.RequestURI, http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := &client{
		http:    server.Client(),
		audit:   &recordingAudit{},
		baseURL: server.URL,
		backoff: 1 * time.Millisecond,
		retries: 3,
	}

	ctx := corr.WithToken(context.Background(), "tok-slash")
	if _, err := c.LookupContract(ctx, ContractQuery{ContractName: "postgres/orders_pg"}); err != nil {
		t.Fatalf("LookupContract: %v", err)
	}

	const want = "/contracts/_doc/postgres%2Forders_pg"
	mu.Lock()
	got := gotContractTarget
	mu.Unlock()
	if got != want {
		t.Errorf("contract GET target = %q, want %q (the slash must be percent-encoded so ES treats the name as one _id)", got, want)
	}
}
