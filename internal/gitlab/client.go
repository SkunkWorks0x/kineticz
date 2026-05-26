package gitlab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/skunkworks0x/kineticz/internal/httputil"
)

// Client exposes the two GitLab REST operations the commit orchestrator
// needs. They are split so the orchestrator can audit COMMIT_OK and
// MR_CREATED separately and recover from MR creation failure without
// re-pushing the commit.
type Client interface {
	CreateCommit(ctx context.Context, req CommitRequest) (string, error)
	CreateMR(ctx context.Context, req MRRequest) (*MRResult, error)
}

// CommitRequest creates a single-file commit on SourceBranch, branching from
// TargetBranch if SourceBranch does not already exist. The commit replaces
// FilePath's contents with FileContent (the caller has already applied the
// diff). CorrelationToken is echoed in the X-Correlation-Token request header.
type CommitRequest struct {
	ProjectID        string
	SourceBranch     string
	TargetBranch     string
	FilePath         string
	FileContent      []byte
	CommitMessage    string
	CorrelationToken string
}

// MRRequest opens a merge request from SourceBranch into TargetBranch. The
// CorrelationToken is prepended to Description as an X-Correlation-Token line
// AND echoed in the request header.
type MRRequest struct {
	ProjectID        string
	SourceBranch     string
	TargetBranch     string
	Title            string
	Description      string
	CorrelationToken string
}

type MRResult struct {
	MRIID  int
	MRURL  string
}

type GitLabError struct {
	StatusCode int
	Body       string
}

func (e *GitLabError) Error() string {
	return fmt.Sprintf("gitlab: HTTP %d: %s", e.StatusCode, e.Body)
}

var (
	ErrGitLabUnavailable = errors.New("gitlab: service unavailable")
	ErrMergeConflict     = errors.New("gitlab: merge conflict")
)

type httpClient struct {
	http    *http.Client
	baseURL string
	token   string
	backoff time.Duration
	retries int
}

// NewHTTPClient constructs a GitLab Client over the given HTTP transport.
// token is a GitLab personal access token or project access token with api
// scope. baseURL defaults to https://gitlab.com or points at self-hosted /
// httptest.
func NewHTTPClient(h *http.Client, baseURL, token string) *httpClient {
	return &httpClient{
		http:    h,
		baseURL: baseURL,
		token:   token,
		backoff: 100 * time.Millisecond,
		retries: 3,
	}
}

func (c *httpClient) CreateCommit(ctx context.Context, req CommitRequest) (string, error) {
	body, err := json.Marshal(map[string]any{
		"branch":         req.SourceBranch,
		"start_branch":   req.TargetBranch,
		"commit_message": req.CommitMessage,
		"actions": []map[string]any{{
			"action":    "update",
			"file_path": req.FilePath,
			"content":   string(req.FileContent),
		}},
	})
	if err != nil {
		return "", fmt.Errorf("gitlab: marshal commit: %w", err)
	}
	path := fmt.Sprintf("/api/v4/projects/%s/repository/commits", url.PathEscape(req.ProjectID))
	rb, status, err := c.do(ctx, http.MethodPost, path, body, req.CorrelationToken)
	if err != nil {
		return "", err
	}
	if status == http.StatusConflict {
		return "", fmt.Errorf("%w: %s", ErrMergeConflict, string(rb))
	}
	if status >= 400 {
		return "", &GitLabError{StatusCode: status, Body: string(rb)}
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rb, &resp); err != nil {
		return "", fmt.Errorf("gitlab: decode commit response: %w", err)
	}
	return resp.ID, nil
}

func (c *httpClient) CreateMR(ctx context.Context, req MRRequest) (*MRResult, error) {
	description := req.Description
	if req.CorrelationToken != "" {
		description = fmt.Sprintf("X-Correlation-Token: %s\n\n%s", req.CorrelationToken, description)
	}
	body, err := json.Marshal(map[string]any{
		"source_branch": req.SourceBranch,
		"target_branch": req.TargetBranch,
		"title":         req.Title,
		"description":   description,
	})
	if err != nil {
		return nil, fmt.Errorf("gitlab: marshal MR: %w", err)
	}
	path := fmt.Sprintf("/api/v4/projects/%s/merge_requests", url.PathEscape(req.ProjectID))
	rb, status, err := c.do(ctx, http.MethodPost, path, body, req.CorrelationToken)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &GitLabError{StatusCode: status, Body: string(rb)}
	}
	var resp struct {
		IID    int    `json:"iid"`
		WebURL string `json:"web_url"`
	}
	if err := json.Unmarshal(rb, &resp); err != nil {
		return nil, fmt.Errorf("gitlab: decode MR response: %w", err)
	}
	return &MRResult{MRIID: resp.IID, MRURL: resp.WebURL}, nil
}

func (c *httpClient) do(ctx context.Context, method, path string, body []byte, correlationToken string) ([]byte, int, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, 0, fmt.Errorf("gitlab: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("PRIVATE-TOKEN", c.token)
	if correlationToken != "" {
		req.Header.Set("X-Correlation-Token", correlationToken)
	}
	resp, err := httputil.Do(ctx, c.http, req, c.retries, c.backoff)
	if err != nil {
		if errors.Is(err, httputil.ErrUnavailable) {
			return nil, 0, fmt.Errorf("%w: %v", ErrGitLabUnavailable, err)
		}
		return nil, 0, err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("gitlab: read body: %w", err)
	}
	return rb, resp.StatusCode, nil
}
