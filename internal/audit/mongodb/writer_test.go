package mongodb

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"testing"

	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
)

// fakeStore is an in-memory chainStore for tests. InTransaction provides
// serializable semantics via mu; entries are appended to a slice in order.
type fakeStore struct {
	mu          sync.Mutex
	entries     []*audit.Entry
	insertErr   error
	latestErr   error
	insertCalls int
}

func (s *fakeStore) Latest(_ context.Context) (*audit.Entry, error) {
	if s.latestErr != nil {
		return nil, s.latestErr
	}
	if len(s.entries) == 0 {
		return nil, ErrEmpty
	}
	return s.entries[len(s.entries)-1], nil
}

func (s *fakeStore) Insert(_ context.Context, e *audit.Entry) error {
	s.insertCalls++
	if s.insertErr != nil {
		return s.insertErr
	}
	s.entries = append(s.entries, e)
	return nil
}

func (s *fakeStore) InTransaction(ctx context.Context, fn func(ctx context.Context, store chainStore) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(ctx, s)
}

func TestWriterAppend(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	cases := []struct {
		name      string
		setup     func() *fakeStore
		appends   int
		wantErrIs error
		assert    func(t *testing.T, fs *fakeStore)
	}{
		{
			name:    "genesis_entry_has_nil_PreviousHash",
			setup:   func() *fakeStore { return &fakeStore{} },
			appends: 1,
			assert: func(t *testing.T, fs *fakeStore) {
				if len(fs.entries) != 1 {
					t.Fatalf("entries = %d, want 1", len(fs.entries))
				}
				if fs.entries[0].PreviousHash != nil {
					t.Errorf("genesis PreviousHash = %x, want nil", fs.entries[0].PreviousHash)
				}
				if err := audit.Verify(*fs.entries[0], nil, pub); err != nil {
					t.Errorf("genesis Verify failed: %v", err)
				}
			},
		},
		{
			name:    "three_entries_form_verifiable_chain",
			setup:   func() *fakeStore { return &fakeStore{} },
			appends: 3,
			assert: func(t *testing.T, fs *fakeStore) {
				if len(fs.entries) != 3 {
					t.Fatalf("entries = %d, want 3", len(fs.entries))
				}
				var prev []byte
				for i, e := range fs.entries {
					if err := audit.Verify(*e, prev, pub); err != nil {
						t.Errorf("entry %d Verify failed: %v", i, err)
					}
					prev = e.Hash
				}
			},
		},
		{
			name:      "store_insert_error_propagates",
			setup:     func() *fakeStore { return &fakeStore{insertErr: errors.New("insert failed")} },
			appends:   1,
			wantErrIs: nil,
			assert: func(t *testing.T, fs *fakeStore) {
				if len(fs.entries) != 0 {
					t.Errorf("entries = %d, want 0", len(fs.entries))
				}
				if fs.insertCalls != 1 {
					t.Errorf("insertCalls = %d, want 1", fs.insertCalls)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := tc.setup()
			w := NewWriter(fs, priv)
			ctx := corr.WithToken(context.Background(), "tok-test")
			for i := 0; i < tc.appends; i++ {
				err := w.Append(ctx, "TEST_ACTION", []byte("payload"))
				if tc.name == "store_insert_error_propagates" {
					if err == nil {
						t.Fatalf("expected error, got nil")
					}
					break
				}
				if err != nil {
					t.Fatalf("Append %d: unexpected error: %v", i, err)
				}
			}
			tc.assert(t, fs)
		})
	}
}

func TestWriterConcurrentAppendsAreSerialized(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	fs := &fakeStore{}
	w := NewWriter(fs, priv)
	ctx := corr.WithToken(context.Background(), "tok-concurrent")

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := w.Append(ctx, "CONCURRENT", []byte("p")); err != nil {
				t.Errorf("Append: %v", err)
			}
		}()
	}
	wg.Wait()

	if len(fs.entries) != n {
		t.Fatalf("entries = %d, want %d", len(fs.entries), n)
	}
	var prev []byte
	for i, e := range fs.entries {
		if i > 0 && string(e.PreviousHash) == "" {
			t.Errorf("entry %d has empty PreviousHash", i)
		}
		if i > 0 && string(prev) != string(e.PreviousHash) {
			t.Errorf("entry %d chain broken: prev=%x got %x", i, prev, e.PreviousHash)
		}
		prev = e.Hash
	}
}

func TestLoadHead(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	t.Run("empty_chain_returns_ErrEmpty", func(t *testing.T) {
		fs := &fakeStore{}
		w := NewWriter(fs, priv)
		_, err := w.LoadHead(context.Background(), pub)
		if !errors.Is(err, ErrEmpty) {
			t.Fatalf("err = %v, want ErrEmpty", err)
		}
	})

	t.Run("valid_head_verifies", func(t *testing.T) {
		fs := &fakeStore{}
		w := NewWriter(fs, priv)
		ctx := corr.WithToken(context.Background(), "tok-load")
		if err := w.Append(ctx, "BOOTSTRAP", []byte("first")); err != nil {
			t.Fatalf("Append: %v", err)
		}
		head, err := w.LoadHead(ctx, pub)
		if err != nil {
			t.Fatalf("LoadHead: %v", err)
		}
		if head.Action != "BOOTSTRAP" {
			t.Errorf("Action = %q, want BOOTSTRAP", head.Action)
		}
	})

	t.Run("tampered_head_fails_verification", func(t *testing.T) {
		fs := &fakeStore{}
		w := NewWriter(fs, priv)
		ctx := corr.WithToken(context.Background(), "tok-tamper")
		if err := w.Append(ctx, "BOOTSTRAP", []byte("first")); err != nil {
			t.Fatalf("Append: %v", err)
		}
		fs.entries[0].Payload = []byte("tampered")
		_, err := w.LoadHead(ctx, pub)
		if err == nil {
			t.Fatal("expected verification error, got nil")
		}
		if !errors.Is(err, audit.ErrHashMismatch) {
			t.Errorf("err = %v, want ErrHashMismatch", err)
		}
	})
}
