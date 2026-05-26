package corr

import "context"

type ctxKey struct{}

func WithToken(ctx context.Context, t CorrelationToken) context.Context {
	return context.WithValue(ctx, ctxKey{}, t)
}

func FromContext(ctx context.Context) (CorrelationToken, bool) {
	t, ok := ctx.Value(ctxKey{}).(CorrelationToken)
	return t, ok
}
