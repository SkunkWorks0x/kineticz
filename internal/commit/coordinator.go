package commit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/bluekeyes/go-gitdiff/gitdiff"

	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/gitlab"
)

// Coordinator applies an approved patch to a file in a GitLab project and
// opens a merge request. Each step (commit + MR) emits a distinct audit
// entry so the ledger can pinpoint which half of the operation failed.
type Coordinator struct {
	gl    gitlab.Client
	audit audit.Writer
}

func New(gl gitlab.Client, aw audit.Writer) *Coordinator {
	return &Coordinator{gl: gl, audit: aw}
}

// Request is what ApplyAndOpenMR consumes. OriginalFile is the pre-patch
// content of FilePath; the coordinator parses Diff with go-gitdiff and
// applies it locally before pushing the resulting file content to GitLab.
type Request struct {
	ProjectID     string
	TargetBranch  string
	FilePath      string
	OriginalFile  []byte
	Diff          []byte
	CommitMessage string
	MRTitle       string
	MRDescription string
}

// Result is the post-MR view: GitLab's commit SHA, MR internal ID and URL,
// and the source branch name (which downstream tooling may need for cleanup).
type Result struct {
	MRIID     int
	MRURL     string
	CommitSHA string
	Branch    string
}

// ApplyAndOpenMR applies the diff, commits the result, then opens an MR.
// Audit sequence on success: COMMIT_OK, MR_CREATED. On commit failure:
// COMMIT_FAILED. On MR failure after commit: COMMIT_OK + MR_FAILED (the
// commit landed and must be cleaned up out of band).
func (c *Coordinator) ApplyAndOpenMR(ctx context.Context, req Request) (*Result, error) {
	patched, err := applyDiff(req.OriginalFile, req.Diff)
	if err != nil {
		_ = c.writeAudit(ctx, "COMMIT_FAILED", req.FilePath, err.Error(), 0, "")
		return nil, fmt.Errorf("commit: apply diff: %w", err)
	}

	token, _ := corr.FromContext(ctx)
	sourceBranch := branchName(token)

	sha, err := c.gl.CreateCommit(ctx, gitlab.CommitRequest{
		ProjectID:        req.ProjectID,
		SourceBranch:     sourceBranch,
		TargetBranch:     req.TargetBranch,
		FilePath:         req.FilePath,
		FileContent:      patched,
		CommitMessage:    req.CommitMessage,
		CorrelationToken: string(token),
	})
	if err != nil {
		_ = c.writeAudit(ctx, "COMMIT_FAILED", req.FilePath, err.Error(), 0, "")
		return nil, fmt.Errorf("commit: create commit: %w", err)
	}
	_ = c.writeAudit(ctx, "COMMIT_OK", req.FilePath, "", 0, sha)

	mr, err := c.gl.CreateMR(ctx, gitlab.MRRequest{
		ProjectID:        req.ProjectID,
		SourceBranch:     sourceBranch,
		TargetBranch:     req.TargetBranch,
		Title:            req.MRTitle,
		Description:      req.MRDescription,
		CorrelationToken: string(token),
	})
	if err != nil {
		_ = c.writeAudit(ctx, "MR_FAILED", req.FilePath, err.Error(), 0, sha)
		return nil, fmt.Errorf("commit: create MR (commit %s already pushed): %w", sha, err)
	}
	_ = c.writeAudit(ctx, "MR_CREATED", req.FilePath, "", mr.MRIID, sha)

	return &Result{MRIID: mr.MRIID, MRURL: mr.MRURL, CommitSHA: sha, Branch: sourceBranch}, nil
}

func applyDiff(orig, diff []byte) ([]byte, error) {
	files, _, err := gitdiff.Parse(bytes.NewReader(diff))
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if len(files) != 1 {
		return nil, fmt.Errorf("expected exactly 1 file in diff, got %d", len(files))
	}
	var out bytes.Buffer
	if err := gitdiff.Apply(&out, bytes.NewReader(orig), files[0]); err != nil {
		return nil, fmt.Errorf("apply: %w", err)
	}
	return out.Bytes(), nil
}

func branchName(token corr.CorrelationToken) string {
	if token == "" {
		return "kineticz/auto-patch"
	}
	return "kineticz/" + string(token)
}

func (c *Coordinator) writeAudit(ctx context.Context, action, filePath, errMsg string, mrIID int, sha string) error {
	payload := map[string]any{"file_path": filePath}
	if errMsg != "" {
		payload["error"] = errMsg
	}
	if sha != "" {
		payload["commit_sha"] = sha
	}
	if mrIID > 0 {
		payload["mr_iid"] = mrIID
	}
	body, _ := json.Marshal(payload)
	return c.audit.Append(ctx, action, body)
}
