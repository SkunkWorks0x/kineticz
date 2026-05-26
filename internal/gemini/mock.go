package gemini

import "context"

type Mock struct {
	GenerateFn func(ctx context.Context, req GenerateRequest) (*Response, error)
}

func (m *Mock) Generate(ctx context.Context, req GenerateRequest) (*Response, error) {
	if m.GenerateFn != nil {
		return m.GenerateFn(ctx, req)
	}
	return &Response{}, nil
}
