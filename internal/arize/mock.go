package arize

import "context"

type Mock struct {
	EvaluateFn func(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error)
}

func (m *Mock) Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	if m.EvaluateFn != nil {
		return m.EvaluateFn(ctx, req)
	}
	return &EvaluateResponse{Pass: true}, nil
}
