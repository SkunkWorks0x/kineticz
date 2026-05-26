package commit

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/gitlab"
)

const testDiff = `diff --git a/users.go b/users.go
--- a/users.go
+++ b/users.go
@@ -1,3 +1,3 @@
 line1
-line2
+line2 modified
 line3
`

const testOrig = "line1\nline2\nline3\n"

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

func standardRequest() Request {
	return Request{
		ProjectID:     "kineticz/pipelines",
		TargetBranch:  "main",
		FilePath:      "users.go",
		OriginalFile:  []byte(testOrig),
		Diff:          []byte(testDiff),
		CommitMessage: "Apply Kineticz patch",
		MRTitle:       "Auto-patch users contract",
		MRDescription: "Kineticz-generated patch.",
	}
}

func TestApplyAndOpenMR_HappyPath(t *testing.T) {
	aw := &recordingAudit{}
	var commitCalls, mrCalls int
	gl := &gitlab.Mock{
		CreateCommitFn: func(_ context.Context, req gitlab.CommitRequest) (string, error) {
			commitCalls++
			if string(req.FileContent) != "line1\nline2 modified\nline3\n" {
				t.Errorf("FileContent unexpected: %q", req.FileContent)
			}
			if req.SourceBranch != "kineticz/tok-abc" {
				t.Errorf("SourceBranch = %q, want kineticz/tok-abc", req.SourceBranch)
			}
			return "sha-abc", nil
		},
		CreateMRFn: func(_ context.Context, req gitlab.MRRequest) (*gitlab.MRResult, error) {
			mrCalls++
			if req.CorrelationToken != "tok-abc" {
				t.Errorf("CorrelationToken = %q, want tok-abc", req.CorrelationToken)
			}
			return &gitlab.MRResult{MRIID: 17, MRURL: "https://gitlab.example/mr/17"}, nil
		},
	}
	c := New(gl, aw)
	ctx := corr.WithToken(context.Background(), "tok-abc")
	res, err := c.ApplyAndOpenMR(ctx, standardRequest())
	if err != nil {
		t.Fatalf("ApplyAndOpenMR: %v", err)
	}
	if res.MRIID != 17 || res.CommitSHA != "sha-abc" || res.Branch != "kineticz/tok-abc" {
		t.Errorf("unexpected result: %+v", res)
	}
	if commitCalls != 1 || mrCalls != 1 {
		t.Errorf("commitCalls=%d mrCalls=%d, want 1/1", commitCalls, mrCalls)
	}
	if want := []string{"COMMIT_OK", "MR_CREATED"}; !sameSlice(aw.snapshot(), want) {
		t.Errorf("audits = %v, want %v", aw.snapshot(), want)
	}
}

func TestApplyAndOpenMR_DiffParseError(t *testing.T) {
	aw := &recordingAudit{}
	gl := &gitlab.Mock{
		CreateCommitFn: func(context.Context, gitlab.CommitRequest) (string, error) {
			t.Error("CreateCommit should not be called when diff parse fails")
			return "", nil
		},
	}
	c := New(gl, aw)
	req := standardRequest()
	req.Diff = []byte("not a diff at all")
	_, err := c.ApplyAndOpenMR(corr.WithToken(context.Background(), "tok-x"), req)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if want := []string{"COMMIT_FAILED"}; !sameSlice(aw.snapshot(), want) {
		t.Errorf("audits = %v, want %v", aw.snapshot(), want)
	}
}

func TestApplyAndOpenMR_CommitFails(t *testing.T) {
	aw := &recordingAudit{}
	gl := &gitlab.Mock{
		CreateCommitFn: func(context.Context, gitlab.CommitRequest) (string, error) {
			return "", gitlab.ErrMergeConflict
		},
		CreateMRFn: func(context.Context, gitlab.MRRequest) (*gitlab.MRResult, error) {
			t.Error("CreateMR should not be called when commit fails")
			return nil, nil
		},
	}
	c := New(gl, aw)
	_, err := c.ApplyAndOpenMR(corr.WithToken(context.Background(), "tok-mc"), standardRequest())
	if !errors.Is(err, gitlab.ErrMergeConflict) {
		t.Fatalf("err = %v, want ErrMergeConflict", err)
	}
	if want := []string{"COMMIT_FAILED"}; !sameSlice(aw.snapshot(), want) {
		t.Errorf("audits = %v, want %v", aw.snapshot(), want)
	}
}

func TestApplyAndOpenMR_MRFailsAfterCommit(t *testing.T) {
	aw := &recordingAudit{}
	gl := &gitlab.Mock{
		CreateCommitFn: func(context.Context, gitlab.CommitRequest) (string, error) {
			return "sha-orphan", nil
		},
		CreateMRFn: func(context.Context, gitlab.MRRequest) (*gitlab.MRResult, error) {
			return nil, gitlab.ErrGitLabUnavailable
		},
	}
	c := New(gl, aw)
	_, err := c.ApplyAndOpenMR(corr.WithToken(context.Background(), "tok-mr"), standardRequest())
	if !errors.Is(err, gitlab.ErrGitLabUnavailable) {
		t.Fatalf("err = %v, want ErrGitLabUnavailable", err)
	}
	if want := []string{"COMMIT_OK", "MR_FAILED"}; !sameSlice(aw.snapshot(), want) {
		t.Errorf("audits = %v, want %v", aw.snapshot(), want)
	}
}

func TestBranchName_FallbackWhenNoToken(t *testing.T) {
	aw := &recordingAudit{}
	var observedBranch string
	gl := &gitlab.Mock{
		CreateCommitFn: func(_ context.Context, req gitlab.CommitRequest) (string, error) {
			observedBranch = req.SourceBranch
			return "sha", nil
		},
	}
	c := New(gl, aw)
	// No corr.WithToken on ctx
	_, _ = c.ApplyAndOpenMR(context.Background(), standardRequest())
	if observedBranch != "kineticz/auto-patch" {
		t.Errorf("branch = %q, want kineticz/auto-patch", observedBranch)
	}
}
