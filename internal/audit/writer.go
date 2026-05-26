package audit

import "context"

type Writer interface {
	Append(ctx context.Context, action string, payload []byte) error
}
