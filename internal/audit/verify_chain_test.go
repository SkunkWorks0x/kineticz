package audit

import (
	"crypto/ed25519"
	"strings"
	"testing"
)

func TestVerifyChain(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	_, wrongPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	cases := []struct {
		name        string
		mutate      func(c []Entry) []Entry
		wantValid   bool
		wantFailIdx int
		wantReason  string
	}{
		{
			name:        "intact_chain_passes",
			mutate:      func(c []Entry) []Entry { return c },
			wantValid:   true,
			wantFailIdx: -1,
		},
		{
			name:        "empty_ledger_passes",
			mutate:      func(c []Entry) []Entry { return nil },
			wantValid:   true,
			wantFailIdx: -1,
		},
		{
			name:        "tampered_payload_fails_at_entry",
			mutate:      func(c []Entry) []Entry { c[2].Payload = []byte("forged"); return c },
			wantValid:   false,
			wantFailIdx: 2,
			wantReason:  "hash mismatch",
		},
		{
			name: "resigned_with_wrong_key_fails",
			mutate: func(c []Entry) []Entry {
				c[1].Payload = []byte("forged")
				Chain(&c[1], wrongPriv)
				return c
			},
			wantValid:   false,
			wantFailIdx: 1,
			wantReason:  "invalid signature",
		},
		{
			name:        "mid_chain_truncation_fails",
			mutate:      func(c []Entry) []Entry { return append(c[:2], c[3:]...) },
			wantValid:   false,
			wantFailIdx: 2,
			wantReason:  "broken chain",
		},
		{
			name:        "reordered_entries_fail",
			mutate:      func(c []Entry) []Entry { c[1], c[2] = c[2], c[1]; return c },
			wantValid:   false,
			wantFailIdx: 1,
			wantReason:  "broken chain",
		},
		{
			// Tail truncation leaves an internally valid shorter chain. The walk
			// cannot detect it; that requires an external head anchor. This case
			// pins the boundary so nobody believes the walk covers it.
			name:        "tail_truncation_passes_walk_boundary",
			mutate:      func(c []Entry) []Entry { return c[:3] },
			wantValid:   true,
			wantFailIdx: -1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chain := tc.mutate(buildChain(t, 5, priv))
			rep := VerifyChain(chain, pub)
			if rep.Valid != tc.wantValid {
				t.Fatalf("Valid = %v, want %v (report: %+v)", rep.Valid, tc.wantValid, rep)
			}
			if rep.FailedIndex != tc.wantFailIdx {
				t.Errorf("FailedIndex = %d, want %d", rep.FailedIndex, tc.wantFailIdx)
			}
			if rep.Entries != len(chain) {
				t.Errorf("Entries = %d, want %d", rep.Entries, len(chain))
			}
			if tc.wantReason != "" && !strings.Contains(rep.Reason, tc.wantReason) {
				t.Errorf("Reason = %q, want substring %q", rep.Reason, tc.wantReason)
			}
		})
	}
}
