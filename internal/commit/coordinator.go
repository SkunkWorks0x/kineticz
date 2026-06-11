package commit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/bluekeyes/go-gitdiff/gitdiff"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/skunkworks0x/kineticz/internal/arize"
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

// Request is what ApplyAndOpenMR consumes. FileContent is the post-patch
// content of FilePath; the caller (orchestrator) is responsible for applying
// the diff to the original via the public ApplyDiff helper before invoking.
type Request struct {
	ProjectID     string
	TargetBranch  string
	FilePath      string
	FileContent   []byte
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

// ApplyAndOpenMR pushes FileContent as a single-file commit, then opens an
// MR. Audit sequence on success: COMMIT_OK, MR_CREATED. On commit failure:
// COMMIT_FAILED. On MR failure after commit: COMMIT_OK + MR_FAILED (the
// commit landed and must be cleaned up out of band).
func (c *Coordinator) ApplyAndOpenMR(ctx context.Context, req Request) (*Result, error) {
	token, _ := corr.FromContext(ctx)
	sourceBranch := branchName(token)

	ctx, span := arize.Tracer().Start(ctx, "kineticz.commit")
	defer span.End()
	span.SetAttributes(
		attribute.String("kineticz.correlation_token", string(token)),
		attribute.String("kineticz.file_path", req.FilePath),
		attribute.String("kineticz.source_branch", sourceBranch),
	)

	sha, err := c.gl.CreateCommit(ctx, gitlab.CommitRequest{
		ProjectID:        req.ProjectID,
		SourceBranch:     sourceBranch,
		TargetBranch:     req.TargetBranch,
		FilePath:         req.FilePath,
		FileContent:      req.FileContent,
		CommitMessage:    req.CommitMessage,
		CorrelationToken: string(token),
	})
	if err != nil {
		span.SetStatus(codes.Error, "commit: "+err.Error())
		span.RecordError(err)
		_ = c.writeAudit(ctx, "COMMIT_FAILED", req.FilePath, err.Error(), 0, "")
		return nil, fmt.Errorf("commit: create commit: %w", err)
	}
	span.SetAttributes(attribute.String("kineticz.commit_sha", sha))
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
		span.SetStatus(codes.Error, "mr: "+err.Error())
		span.RecordError(err)
		_ = c.writeAudit(ctx, "MR_FAILED", req.FilePath, err.Error(), 0, sha)
		return nil, fmt.Errorf("commit: create MR (commit %s already pushed): %w", sha, err)
	}
	span.SetAttributes(
		attribute.String("kineticz.gitlab_mr_url", mr.MRURL),
		attribute.Int("kineticz.mr_iid", mr.MRIID),
	)
	_ = c.writeAudit(ctx, "MR_CREATED", req.FilePath, "", mr.MRIID, sha)

	return &Result{MRIID: mr.MRIID, MRURL: mr.MRURL, CommitSHA: sha, Branch: sourceBranch}, nil
}

// ApplyDiff parses a single-file unified diff and applies it against orig,
// returning the patched bytes. Exposed so the orchestrator can derive the
// post-patch file content once and pass it to both evaluate and commit
// without re-parsing the diff.
func ApplyDiff(orig, diff []byte) ([]byte, error) {
	files, _, err := gitdiff.Parse(bytes.NewReader(diff))
	if err != nil {
		return nil, fmt.Errorf("commit: parse diff: %w", err)
	}
	if len(files) != 1 {
		return nil, fmt.Errorf("commit: expected exactly 1 file in diff, got %d", len(files))
	}
	if err := reanchorFragments(files[0], orig); err != nil {
		return nil, fmt.Errorf("commit: re-anchor diff: %w", err)
	}
	var out bytes.Buffer
	if err := gitdiff.Apply(&out, bytes.NewReader(orig), files[0]); err != nil {
		return nil, fmt.Errorf("commit: apply diff: %w", err)
	}
	return out.Bytes(), nil
}

// reanchorFragments pins each fragment to the line where its old-side content
// (context and deletion lines) occurs in orig, overriding the model's unreliable
// @@ line numbers. The content is byte-correct but the line numbers drift, so the
// strict applier rejects every hunk. Re-anchoring corrects the start; it fails
// closed when a hunk matches zero positions, more than one, or has no old-side
// content to anchor on, so a wrong patch never lands.
func reanchorFragments(f *gitdiff.File, orig []byte) error {
	var prevStart int64
	for i, frag := range f.TextFragments {
		var block []byte
		for _, ln := range frag.Lines {
			if ln.Old() {
				block = append(block, ln.Line...)
			}
		}
		if len(block) == 0 {
			return fmt.Errorf("hunk %d: empty old-side block, no context to anchor", i)
		}
		starts := lineMatches(orig, block)
		if len(starts) != 1 {
			return fmt.Errorf("hunk %d: old-side block matched %d positions, want exactly 1", i, len(starts))
		}
		if starts[0] <= prevStart {
			return fmt.Errorf("hunk %d: re-anchored start %d not after previous hunk start %d", i, starts[0], prevStart)
		}
		frag.OldPosition = starts[0]
		prevStart = starts[0]
	}
	return nil
}

// lineMatches returns the 1-based line numbers where block begins at a line
// boundary in orig and matches byte-exactly. block is a run of whole source lines
// (each carrying its newline), so a boundary-anchored byte match is an exact line
// match.
func lineMatches(orig, block []byte) []int64 {
	var out []int64
	off := 0
	lineNo := int64(1)
	for off+len(block) <= len(orig) {
		if bytes.Equal(orig[off:off+len(block)], block) {
			out = append(out, lineNo)
		}
		nl := bytes.IndexByte(orig[off:], '\n')
		if nl < 0 {
			break
		}
		off += nl + 1
		lineNo++
	}
	return out
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
