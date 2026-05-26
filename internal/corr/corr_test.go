package corr

import (
	"sync"
	"testing"
)

func TestNew_MonotonicSequential(t *testing.T) {
	const n = 10_000
	tokens := make([]CorrelationToken, n)
	for i := range tokens {
		tokens[i] = New()
	}
	for i := 1; i < n; i++ {
		if Compare(tokens[i-1], tokens[i]) >= 0 {
			t.Fatalf("non-monotonic at %d: %q !< %q", i, tokens[i-1], tokens[i])
		}
	}
}

func TestNew_UniqueConcurrent(t *testing.T) {
	const n = 10_000
	tokens := make([]CorrelationToken, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			tokens[i] = New()
		}(i)
	}
	wg.Wait()

	seen := make(map[CorrelationToken]struct{}, n)
	for _, tok := range tokens {
		if _, dup := seen[tok]; dup {
			t.Fatalf("duplicate token across goroutines: %q", tok)
		}
		seen[tok] = struct{}{}
	}
}

func TestCompare(t *testing.T) {
	cases := []struct {
		name string
		a, b CorrelationToken
		want int
	}{
		{"less", "00000000000000000001-00000000000000000001", "00000000000000000001-00000000000000000002", -1},
		{"equal", "00000000000000000001-00000000000000000001", "00000000000000000001-00000000000000000001", 0},
		{"greater_by_nanos", "00000000000000000002-00000000000000000001", "00000000000000000001-00000000000000000099", 1},
		{"greater_by_counter", "00000000000000000005-00000000000000000010", "00000000000000000005-00000000000000000009", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Compare(tc.a, tc.b); got != tc.want {
				t.Fatalf("Compare(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
