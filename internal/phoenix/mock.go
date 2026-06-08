package phoenix

import "context"

type Mock struct {
	QuerySpansFn func(ctx context.Context, q SpanQuery) ([]Span, error)
	CloseFn      func() error
}

func (m *Mock) QuerySpans(ctx context.Context, q SpanQuery) ([]Span, error) {
	if m.QuerySpansFn != nil {
		return m.QuerySpansFn(ctx, q)
	}
	return nil, nil
}

func (m *Mock) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}
