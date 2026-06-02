package repair

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/bluekeyes/go-gitdiff/gitdiff"

	"github.com/skunkworks0x/kineticz/internal/commit"
	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/dynatrace"
	"github.com/skunkworks0x/kineticz/internal/elastic"
	"github.com/skunkworks0x/kineticz/internal/engine/diagnose"
	"github.com/skunkworks0x/kineticz/internal/gemini"
)

type recordingAudit struct {
	mu       sync.Mutex
	entries  []recordedEntry
}

type recordedEntry struct {
	Action  string
	Thought string
}

func (r *recordingAudit) Append(_ context.Context, action string, _ []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, recordedEntry{Action: action})
	return nil
}

func (r *recordingAudit) AppendWithThought(_ context.Context, action string, _ []byte, thought string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, recordedEntry{Action: action, Thought: thought})
	return nil
}

func (r *recordingAudit) actions() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.entries))
	for i, e := range r.entries {
		out[i] = e.Action
	}
	return out
}

type fakeTarget struct {
	content []byte
	err     error
	reads   int
}

func (f *fakeTarget) Read(_ context.Context, _ string) ([]byte, error) {
	f.reads++
	if f.err != nil {
		return nil, f.err
	}
	return f.content, nil
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

// validDiagnosis returns a passing DiagnosisResult for use across tests.
func validDiagnosis() *diagnose.DiagnosisResult {
	return &diagnose.DiagnosisResult{
		ContractContext: &elastic.ContractContext{
			YAMLDefinition: "name: users_v1\n",
			MitigationPatterns: []elastic.Mitigation{
				{DiffID: "diff-001", Score: 0.5, Summary: "add timestamp"},
			},
			RRFConfidence: 0.5,
		},
		ConsumerHealth:   []dynatrace.ConsumerHealth{{Consumer: "svc-a", ErrorRate: 0.1, LatencyP95Ms: 120}},
		CorrelationToken: "tok-test",
	}
}

// responseWithDiff builds a Gemini Response containing the given diff text
// plus an optional thinking block.
func responseWithDiff(thought, diff string) *gemini.Response {
	parts := []gemini.Part{}
	if thought != "" {
		parts = append(parts, gemini.Part{Text: thought, Thought: true})
	}
	parts = append(parts, gemini.Part{Text: diff})
	return &gemini.Response{
		Candidates: []gemini.Candidate{{Content: gemini.Content{Parts: parts}}},
	}
}

func TestValidateDiff(t *testing.T) {
	cases := []struct {
		name       string
		fixture    string
		wantReason string
	}{
		{"valid_single_file_passes", "valid_single_file.diff", ""},
		{"multi_file_rejected", "multi_file.diff", "multi_file"},
		{"binary_rejected", "binary.diff", "binary"},
		{"empty_hunks_rejected", "empty_hunks.diff", "empty_hunks"},
		{"path_traversal_rejected", "path_traversal.diff", "path_traversal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			files, _, err := gitdiff.Parse(bytes.NewReader(loadFixture(t, tc.fixture)))
			if err != nil {
				t.Fatalf("parse fixture: %v", err)
			}
			if len(files) == 0 {
				t.Fatal("fixture parsed to zero files")
			}
			got := validateDiff(files)
			if got != tc.wantReason {
				t.Errorf("validateDiff = %q, want %q", got, tc.wantReason)
			}
		})
	}
}

func TestRepair_HappyPath(t *testing.T) {
	validDiff := loadFixture(t, "valid_single_file.diff")
	aw := &recordingAudit{}
	tr := &fakeTarget{content: loadFixture(t, "users.go.src")}
	gm := &gemini.Mock{
		GenerateFn: func(_ context.Context, _ gemini.GenerateRequest) (*gemini.Response, error) {
			return responseWithDiff("Step 1: inspect schema.", string(validDiff)), nil
		},
	}
	c := New(gm, aw, tr, commit.ApplyDiff)

	ctx := corr.WithToken(context.Background(), "tok-happy")
	res, err := c.Repair(ctx, validDiagnosis(), "internal/pipeline/users.go")
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if res.Iterations != 1 {
		t.Errorf("Iterations = %d, want 1", res.Iterations)
	}
	if res.Thought == "" {
		t.Error("Thought is empty, expected Gemini reasoning")
	}
	if !bytes.Equal(res.PatchDiff, validDiff) {
		t.Errorf("PatchDiff did not match fixture")
	}
	want := []string{"REPAIR_ATTEMPT", "REPAIR_APPROVED"}
	if got := aw.actions(); !sameSlice(got, want) {
		t.Errorf("audit actions = %v, want %v", got, want)
	}
}

func TestRepair_RejectedThenApproved(t *testing.T) {
	multi := string(loadFixture(t, "multi_file.diff"))
	valid := string(loadFixture(t, "valid_single_file.diff"))
	var call int
	gm := &gemini.Mock{
		GenerateFn: func(_ context.Context, _ gemini.GenerateRequest) (*gemini.Response, error) {
			call++
			if call == 1 {
				return responseWithDiff("reasoning 1", multi), nil
			}
			return responseWithDiff("reasoning 2", valid), nil
		},
	}
	aw := &recordingAudit{}
	tr := &fakeTarget{content: loadFixture(t, "users.go.src")}
	c := New(gm, aw, tr, commit.ApplyDiff)

	res, err := c.Repair(corr.WithToken(context.Background(), "tok-retry"), validDiagnosis(), "internal/pipeline/users.go")
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if res.Iterations != 2 {
		t.Errorf("Iterations = %d, want 2", res.Iterations)
	}
	if tr.reads != 2 {
		t.Errorf("target reads = %d, want 2 (refresh per iteration)", tr.reads)
	}
	want := []string{"REPAIR_ATTEMPT", "REPAIR_REJECTED", "REPAIR_ATTEMPT", "REPAIR_APPROVED"}
	if got := aw.actions(); !sameSlice(got, want) {
		t.Errorf("audit actions = %v, want %v", got, want)
	}
}

func TestRepair_TwoConsecutiveEmpty(t *testing.T) {
	gm := &gemini.Mock{
		GenerateFn: func(_ context.Context, _ gemini.GenerateRequest) (*gemini.Response, error) {
			return responseWithDiff("reasoning", ""), nil
		},
	}
	aw := &recordingAudit{}
	tr := &fakeTarget{content: []byte("package pipeline\n")}
	c := New(gm, aw, tr, commit.ApplyDiff)
	_, err := c.Repair(corr.WithToken(context.Background(), "tok-empty"), validDiagnosis(), "internal/pipeline/users.go")
	if !errors.Is(err, ErrTwoConsecutiveEmpty) {
		t.Fatalf("err = %v, want ErrTwoConsecutiveEmpty", err)
	}
}

func TestRepair_MaxIterationsExceeded(t *testing.T) {
	multi := string(loadFixture(t, "multi_file.diff"))
	gm := &gemini.Mock{
		GenerateFn: func(_ context.Context, _ gemini.GenerateRequest) (*gemini.Response, error) {
			return responseWithDiff("reasoning", multi), nil
		},
	}
	aw := &recordingAudit{}
	tr := &fakeTarget{content: []byte("package pipeline\n")}
	c := New(gm, aw, tr, commit.ApplyDiff)
	_, err := c.Repair(corr.WithToken(context.Background(), "tok-max"), validDiagnosis(), "internal/pipeline/users.go")
	if !errors.Is(err, ErrMaxIterationsExceeded) {
		t.Fatalf("err = %v, want ErrMaxIterationsExceeded", err)
	}
	if tr.reads != 4 {
		t.Errorf("target reads = %d, want 4 (one per iteration)", tr.reads)
	}
}

func TestRepair_InvalidDiagnosisResult(t *testing.T) {
	gm := &gemini.Mock{}
	aw := &recordingAudit{}
	tr := &fakeTarget{}
	c := New(gm, aw, tr, commit.ApplyDiff)
	bad := &diagnose.DiagnosisResult{} // missing everything
	_, err := c.Repair(context.Background(), bad, "x")
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestRepair_TargetReadError(t *testing.T) {
	gm := &gemini.Mock{}
	aw := &recordingAudit{}
	tr := &fakeTarget{err: fmt.Errorf("disk gone")}
	c := New(gm, aw, tr, commit.ApplyDiff)
	_, err := c.Repair(corr.WithToken(context.Background(), "tok-disk"), validDiagnosis(), "x")
	if err == nil {
		t.Fatal("expected target read error")
	}
}

func TestRepair_StripsMarkdownFences(t *testing.T) {
	validDiff := string(loadFixture(t, "valid_single_file.diff"))
	fenced := "Here is the patch:\n\n```diff\n" + validDiff + "```\n"
	gm := &gemini.Mock{
		GenerateFn: func(_ context.Context, _ gemini.GenerateRequest) (*gemini.Response, error) {
			return &gemini.Response{Candidates: []gemini.Candidate{{
				Content: gemini.Content{Parts: []gemini.Part{{Text: fenced}}},
			}}}, nil
		},
	}
	aw := &recordingAudit{}
	tr := &fakeTarget{content: loadFixture(t, "users.go.src")}
	c := New(gm, aw, tr, commit.ApplyDiff)
	res, err := c.Repair(corr.WithToken(context.Background(), "tok-md"), validDiagnosis(), "x")
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if res.Iterations != 1 {
		t.Errorf("Iterations = %d, want 1", res.Iterations)
	}
}

// An apply conflict on the first attempt is a soft rejection: the loop feeds
// the literal gitdiff error back and retries. The second clean diff approves.
func TestRepair_ApplyConflictRetries(t *testing.T) {
	src := loadFixture(t, "users.go.src")
	conflict := string(loadFixture(t, "conflict_context.diff"))
	valid := string(loadFixture(t, "valid_single_file.diff"))
	var prompts []string
	var call int
	gm := &gemini.Mock{
		GenerateFn: func(_ context.Context, req gemini.GenerateRequest) (*gemini.Response, error) {
			prompts = append(prompts, req.UserPrompt)
			call++
			if call == 1 {
				return responseWithDiff("first attempt", conflict), nil
			}
			return responseWithDiff("second attempt", valid), nil
		},
	}
	aw := &recordingAudit{}
	tr := &fakeTarget{content: src}
	c := New(gm, aw, tr, commit.ApplyDiff)

	res, err := c.Repair(corr.WithToken(context.Background(), "tok-conflict"), validDiagnosis(), "internal/pipeline/users.go")
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if res.Iterations != 2 {
		t.Errorf("Iterations = %d, want 2", res.Iterations)
	}
	if len(prompts) < 2 || !strings.Contains(prompts[1], "fragment line does not match src line") {
		t.Errorf("iteration-2 prompt missing apply-conflict feedback; prompts = %q", prompts)
	}
	want := []string{"REPAIR_ATTEMPT", "REPAIR_REJECTED", "REPAIR_ATTEMPT", "REPAIR_APPROVED"}
	if got := aw.actions(); !sameSlice(got, want) {
		t.Errorf("audit actions = %v, want %v", got, want)
	}
}

// A clean diff applies inside the loop; Result carries the source it applied
// against and the patched bytes, so the orchestrator skips a re-read.
func TestRepair_AppliesPatchInLoop(t *testing.T) {
	src := loadFixture(t, "users.go.src")
	want := loadFixture(t, "users.go.patched")
	valid := string(loadFixture(t, "valid_single_file.diff"))
	gm := &gemini.Mock{
		GenerateFn: func(_ context.Context, _ gemini.GenerateRequest) (*gemini.Response, error) {
			return responseWithDiff("apply this", valid), nil
		},
	}
	tr := &fakeTarget{content: src}
	c := New(gm, &recordingAudit{}, tr, commit.ApplyDiff)

	res, err := c.Repair(corr.WithToken(context.Background(), "tok-apply"), validDiagnosis(), "internal/pipeline/users.go")
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if !bytes.Equal(res.Patched, want) {
		t.Errorf("Patched mismatch:\n got %q\nwant %q", res.Patched, want)
	}
	if !bytes.Equal(res.Orig, src) {
		t.Errorf("Orig did not equal the read target")
	}
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
