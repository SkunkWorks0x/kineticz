package gitlab

import "context"

type Mock struct {
	CreateCommitFn    func(ctx context.Context, req CommitRequest) (string, error)
	CreateMRFn        func(ctx context.Context, req MRRequest) (*MRResult, error)
	GetFileContentFn  func(ctx context.Context, projectID, filePath, ref string) ([]byte, error)
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

func (m *Mock) GetFileContent(ctx context.Context, projectID, filePath, ref string) ([]byte, error) {
	if m.GetFileContentFn != nil {
		return m.GetFileContentFn(ctx, projectID, filePath, ref)
	}
	return nil, nil
}
