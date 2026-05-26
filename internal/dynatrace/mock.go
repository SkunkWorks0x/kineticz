package dynatrace

import "context"

type Mock struct {
	IngestBizeventFn      func(ctx context.Context, eventType string, attrs map[string]any) error
	QueryConsumerHealthFn func(ctx context.Context, syncStartMs, syncEndMs int64) ([]ConsumerHealth, error)
}

func (m *Mock) IngestBizevent(ctx context.Context, eventType string, attrs map[string]any) error {
	if m.IngestBizeventFn != nil {
		return m.IngestBizeventFn(ctx, eventType, attrs)
	}
	return nil
}

func (m *Mock) QueryConsumerHealth(ctx context.Context, syncStartMs, syncEndMs int64) ([]ConsumerHealth, error) {
	if m.QueryConsumerHealthFn != nil {
		return m.QueryConsumerHealthFn(ctx, syncStartMs, syncEndMs)
	}
	return nil, nil
}
