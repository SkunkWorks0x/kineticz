package corr

import (
	"context"
	"testing"
)

func TestWithToken_FromContext(t *testing.T) {
	cases := []struct {
		name      string
		setup     func() context.Context
		wantToken CorrelationToken
		wantOK    bool
	}{
		{
			name:      "background context has no token",
			setup:     context.Background,
			wantToken: "",
			wantOK:    false,
		},
		{
			name: "context with token returns it",
			setup: func() context.Context {
				return WithToken(context.Background(), "tok-abc")
			},
			wantToken: "tok-abc",
			wantOK:    true,
		},
		{
			name: "overwriting token returns latest",
			setup: func() context.Context {
				ctx := WithToken(context.Background(), "tok-first")
				return WithToken(ctx, "tok-second")
			},
			wantToken: "tok-second",
			wantOK:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tok, ok := FromContext(tc.setup())
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if tok != tc.wantToken {
				t.Fatalf("token = %q, want %q", tok, tc.wantToken)
			}
		})
	}
}
