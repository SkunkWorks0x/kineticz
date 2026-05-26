package evaluate

import (
	"context"
	"sync"
	"testing"
	"time"
)

const origPkg = `package pipeline

import "errors"

type User struct {
	ID    int64
	Email string
}

func ParseUser(row map[string]any) (*User, error) {
	if row == nil {
		return nil, errors.New("nil row")
	}
	return &User{ID: row["id"].(int64), Email: row["email"].(string)}, nil
}

func (u *User) FullEmail() string {
	return u.Email
}
`

const patchedPkgAdditive = `package pipeline

import (
	"errors"
	"time"
)

type User struct {
	ID        int64
	Email     string
	CreatedAt time.Time
}

func ParseUser(row map[string]any) (*User, error) {
	if row == nil {
		return nil, errors.New("nil row")
	}
	createdAt, _ := row["created_at"].(time.Time)
	return &User{ID: row["id"].(int64), Email: row["email"].(string), CreatedAt: createdAt}, nil
}

func (u *User) FullEmail() string {
	return u.Email
}
`

const patchedPkgChangedSig = `package pipeline

import "errors"

type User struct {
	ID    int64
	Email string
}

// Removed second return value; changed signature.
func ParseUser(row map[string]any) *User {
	return nil
}

func (u *User) FullEmail() string {
	return u.Email
}
`

const patchedPkgRemovedFunc = `package pipeline

type User struct {
	ID    int64
	Email string
}

func (u *User) FullEmail() string {
	return u.Email
}
`

const patchedPkgBrokenSyntax = `package pipeline

func ParseUser(row map[string]any) (*User, error {
	return nil, nil
}
`

func TestRunLocal(t *testing.T) {
	cases := []struct {
		name        string
		orig        string
		patched     string
		wantVerdict Verdict
		wantReason  string // substring
	}{
		{"additive_change_allowed", origPkg, patchedPkgAdditive, VerdictAllow, ""},
		{"changed_signature_blocked", origPkg, patchedPkgChangedSig, VerdictBlock, "changed_exported_sig: ParseUser"},
		{"removed_function_blocked", origPkg, patchedPkgRemovedFunc, VerdictBlock, "removed_exported_sig: ParseUser"},
		{"unparseable_patch_blocked", origPkg, patchedPkgBrokenSyntax, VerdictBlock, "parse_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := RunLocal([]byte(tc.orig), []byte(tc.patched))
			if r.Verdict != tc.wantVerdict {
				t.Fatalf("Verdict = %v, want %v (reason=%q)", r.Verdict, tc.wantVerdict, r.Reason)
			}
			if tc.wantReason != "" && !contains(r.Reason, tc.wantReason) {
				t.Errorf("Reason = %q, want contains %q", r.Reason, tc.wantReason)
			}
		})
	}
}

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

type fakeIndexer struct {
	mu     sync.Mutex
	called chan struct{}
	shas   []string
}

func newFakeIndexer() *fakeIndexer {
	return &fakeIndexer{called: make(chan struct{}, 4)}
}

func (f *fakeIndexer) Index(_ context.Context, sha string, _ []byte) error {
	f.mu.Lock()
	f.shas = append(f.shas, sha)
	f.mu.Unlock()
	f.called <- struct{}{}
	return nil
}

func (f *fakeIndexer) waitCalls(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-f.called:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for indexer call %d", i+1)
		}
	}
}

func TestGate_LocalBlockEmitsBlockAndIndexes(t *testing.T) {
	aw := &recordingAudit{}
	idx := newFakeIndexer()
	g := New(aw, idx)
	res, err := g.Evaluate(context.Background(), []byte(origPkg), []byte(patchedPkgChangedSig), []byte("diff bytes"))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Verdict != VerdictBlock {
		t.Errorf("Verdict = %v, want VerdictBlock", res.Verdict)
	}
	if want := []string{"EVALUATE_LOCAL_BLOCK"}; !sameSlice(aw.snapshot(), want) {
		t.Errorf("audits = %v, want %v", aw.snapshot(), want)
	}
	idx.waitCalls(t, 1)
}

func TestGate_LocalPassEmitsEvaluatePass(t *testing.T) {
	aw := &recordingAudit{}
	idx := newFakeIndexer()
	g := New(aw, idx)
	res, err := g.Evaluate(context.Background(), []byte(origPkg), []byte(patchedPkgAdditive), []byte("good diff"))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Verdict != VerdictAllow {
		t.Errorf("Verdict = %v, want VerdictAllow", res.Verdict)
	}
	if want := []string{"EVALUATE_PASS"}; !sameSlice(aw.snapshot(), want) {
		t.Errorf("audits = %v, want %v", aw.snapshot(), want)
	}
}

func TestGate_DedupSuppressesRepeats(t *testing.T) {
	aw := &recordingAudit{}
	idx := newFakeIndexer()
	g := New(aw, idx)
	diff := []byte("the same diff bytes")

	// First call: local-block path
	_, _ = g.Evaluate(context.Background(), []byte(origPkg), []byte(patchedPkgChangedSig), diff)
	idx.waitCalls(t, 1)

	// Second call with the same diff: should be deduplicated.
	res2, err := g.Evaluate(context.Background(), []byte(origPkg), []byte(patchedPkgChangedSig), diff)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !res2.Deduplicated {
		t.Error("expected Deduplicated=true on repeat")
	}
	if res2.Verdict != VerdictBlock {
		t.Errorf("Verdict = %v, want VerdictBlock on dedup", res2.Verdict)
	}
	got := aw.snapshot()
	// Only the first call audits; the dedup short-circuit emits no audit.
	if len(got) != 1 || got[0] != "EVALUATE_LOCAL_BLOCK" {
		t.Errorf("audits = %v, want one EVALUATE_LOCAL_BLOCK", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func sameSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
