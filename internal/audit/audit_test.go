package audit

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/skunkworks0x/kineticz/internal/corr"
)

func buildChain(t *testing.T, n int, priv ed25519.PrivateKey) []Entry {
	t.Helper()
	entries := make([]Entry, n)
	var prev []byte
	for i := 0; i < n; i++ {
		entries[i] = Entry{
			ID:               fmt.Sprintf("entry-%d", i),
			CorrelationToken: corr.New(),
			Action:           "TEST_ACTION",
			Payload:          []byte(fmt.Sprintf("payload-%d", i)),
			Thought:          fmt.Sprintf("reasoning step %d: observed signal, chose path A", i),
			PreviousHash:     prev,
			Timestamp:        time.Unix(1700000000+int64(i), 0).UTC(),
		}
		Chain(&entries[i], priv)
		prev = entries[i].Hash
	}
	return entries
}

func verifyAll(chain []Entry, pub ed25519.PublicKey) error {
	var prev []byte
	for i := range chain {
		if err := Verify(chain[i], prev, pub); err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		prev = chain[i].Hash
	}
	return nil
}

func TestChainAndVerify(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(chain []Entry)
		wantErr error
	}{
		{
			name:    "valid chain of 5",
			mutate:  func([]Entry) {},
			wantErr: nil,
		},
		{
			name:    "tampered payload",
			mutate:  func(c []Entry) { c[2].Payload = []byte("forged") },
			wantErr: ErrHashMismatch,
		},
		{
			name:    "tampered thought",
			mutate:  func(c []Entry) { c[2].Thought = "fabricated reasoning" },
			wantErr: ErrHashMismatch,
		},
		{
			name:    "broken chain link",
			mutate:  func(c []Entry) { c[3].PreviousHash = []byte("not-the-real-previous-hash") },
			wantErr: ErrBrokenChain,
		},
		{
			name:    "invalid signature",
			mutate:  func(c []Entry) { c[1].Ed25519Signature[0] ^= 0xff },
			wantErr: ErrBadSignature,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chain := buildChain(t, 5, priv)
			tc.mutate(chain)
			err := verifyAll(chain, pub)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("expected verification to pass, got: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want errors.Is(%v)", err, tc.wantErr)
			}
		})
	}
}
