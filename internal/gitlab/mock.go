package gitlab

import "context"

type Mock struct {
	CreateCommitFn func(ctx context.Context, req CommitRequest) (string, error)
	CreateMRFn     func(ctx context.Context, req MRRequest) (*MRResult, error)
}

func (m *Mock) CreateCommit(ctx context.Context, req CommitRequest) (string, error) {
	if m.CreateCommitFn != nil {
		return m.CreateCommitFn(ctx, req)
	}
	return "deadbeef", nil
}

func (m *Mock) CreateMR(ctx context.Context, req MRRequest) (*MRResult, error) {
	if m.CreateMRFn != nil {
		return m.CreateMRFn(ctx, req)
	}
	return &MRResult{MRIID: 1, MRURL: "https://gitlab.example/mr/1"}, nil
}
