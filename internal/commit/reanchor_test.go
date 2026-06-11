package commit

import (
	"strings"
	"testing"
)

// reBase is the re-anchor target. 1-based line numbers referenced by the diffs:
//
//	3 func scanFact   4 var orderID   5 var amount   6 var createdAt
//	7 return nil      10 func revenue 11 if createdAt.IsZero  12 return 0
const reBase = "package pipeline\n" +
	"\n" +
	"func scanFact() error {\n" +
	"\tvar orderID int64\n" +
	"\tvar amount string\n" +
	"\tvar createdAt time.Time\n" +
	"\treturn nil\n" +
	"}\n" +
	"\n" +
	"func revenue() int {\n" +
	"\tif createdAt.IsZero() {\n" +
	"\t\treturn 0\n" +
	"\t}\n" +
	"\treturn 1\n" +
	"}\n"

const reHdr = "diff --git a/pipeline.go b/pipeline.go\n" +
	"--- a/pipeline.go\n" +
	"+++ b/pipeline.go\n"

func TestApplyDiff_ReanchorsOffsetHunk(t *testing.T) {
	// Content correct, @@ start wrong by +5 (real start is line 4).
	diff := reHdr +
		"@@ -9,4 +9,4 @@\n" +
		" \tvar orderID int64\n" +
		" \tvar amount string\n" +
		"-\tvar createdAt time.Time\n" +
		"+\tvar createdAt sql.NullTime\n" +
		" \treturn nil\n"
	want := strings.Replace(reBase, "\tvar createdAt time.Time\n", "\tvar createdAt sql.NullTime\n", 1)

	got, err := ApplyDiff([]byte(reBase), []byte(diff))
	if err != nil {
		t.Fatalf("ApplyDiff: %v", err)
	}
	if string(got) != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

func TestApplyDiff_ReanchorsOrdersPgPlus5TwoHunks(t *testing.T) {
	// The live orders_pg shape: two hunks, both @@ offset by +5.
	diff := reHdr +
		"@@ -9,4 +9,4 @@\n" +
		" \tvar orderID int64\n" +
		" \tvar amount string\n" +
		"-\tvar createdAt time.Time\n" +
		"+\tvar createdAt sql.NullTime\n" +
		" \treturn nil\n" +
		"@@ -15,3 +15,3 @@\n" +
		" func revenue() int {\n" +
		"-\tif createdAt.IsZero() {\n" +
		"+\tif !createdAt.Valid {\n" +
		" \t\treturn 0\n"
	want := strings.Replace(reBase, "\tvar createdAt time.Time\n", "\tvar createdAt sql.NullTime\n", 1)
	want = strings.Replace(want, "\tif createdAt.IsZero() {\n", "\tif !createdAt.Valid {\n", 1)

	got, err := ApplyDiff([]byte(reBase), []byte(diff))
	if err != nil {
		t.Fatalf("ApplyDiff: %v", err)
	}
	if string(got) != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

func TestApplyDiff_ReanchorsTwoHunksDifferentOffsets(t *testing.T) {
	// Hunk 1 offset +5 (real 4); hunk 2 offset -3 (real 10).
	diff := reHdr +
		"@@ -9,4 +9,4 @@\n" +
		" \tvar orderID int64\n" +
		" \tvar amount string\n" +
		"-\tvar createdAt time.Time\n" +
		"+\tvar createdAt sql.NullTime\n" +
		" \treturn nil\n" +
		"@@ -7,3 +7,3 @@\n" +
		" func revenue() int {\n" +
		"-\tif createdAt.IsZero() {\n" +
		"+\tif !createdAt.Valid {\n" +
		" \t\treturn 0\n"
	want := strings.Replace(reBase, "\tvar createdAt time.Time\n", "\tvar createdAt sql.NullTime\n", 1)
	want = strings.Replace(want, "\tif createdAt.IsZero() {\n", "\tif !createdAt.Valid {\n", 1)

	got, err := ApplyDiff([]byte(reBase), []byte(diff))
	if err != nil {
		t.Fatalf("ApplyDiff: %v", err)
	}
	if string(got) != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

func TestApplyDiff_RejectsNonUniqueOldSideBlock(t *testing.T) {
	orig := "alpha\n\tfoo\n\tbar\nalpha\n\tfoo\n\tbar\n"
	diff := reHdr +
		"@@ -2,2 +2,2 @@\n" +
		"-\tfoo\n" +
		"+\tfoo2\n" +
		" \tbar\n"
	_, err := ApplyDiff([]byte(orig), []byte(diff))
	if err == nil {
		t.Fatal("expected rejection on non-unique old-side block")
	}
	if !strings.Contains(err.Error(), "hunk 0") || !strings.Contains(err.Error(), "matched 2 positions") {
		t.Errorf("error must name hunk and count>1, got: %v", err)
	}
}

func TestApplyDiff_RejectsZeroMatchOldSideBlock(t *testing.T) {
	diff := reHdr +
		"@@ -3,2 +3,2 @@\n" +
		"-\tnonexistent source line\n" +
		"+\treplacement\n" +
		" \tanother missing line\n"
	_, err := ApplyDiff([]byte(reBase), []byte(diff))
	if err == nil {
		t.Fatal("expected rejection on zero-match old-side block")
	}
	if !strings.Contains(err.Error(), "hunk 0") || !strings.Contains(err.Error(), "matched 0 positions") {
		t.Errorf("error must name hunk and count 0, got: %v", err)
	}
}

func TestApplyDiff_RejectsPureInsertionEmptyOldSide(t *testing.T) {
	diff := reHdr +
		"@@ -2,0 +2,2 @@\n" +
		"+inserted line a\n" +
		"+inserted line b\n"
	_, err := ApplyDiff([]byte(reBase), []byte(diff))
	if err == nil {
		t.Fatal("expected rejection on pure-insertion hunk")
	}
	if !strings.Contains(err.Error(), "hunk 0") || !strings.Contains(err.Error(), "empty old-side") {
		t.Errorf("error must name hunk and empty old-side, got: %v", err)
	}
}

func TestApplyDiff_CorrectPositionStillApplies(t *testing.T) {
	// Offset 0: @@ already at the real start line 4. No regression.
	diff := reHdr +
		"@@ -4,4 +4,4 @@\n" +
		" \tvar orderID int64\n" +
		" \tvar amount string\n" +
		"-\tvar createdAt time.Time\n" +
		"+\tvar createdAt sql.NullTime\n" +
		" \treturn nil\n"
	want := strings.Replace(reBase, "\tvar createdAt time.Time\n", "\tvar createdAt sql.NullTime\n", 1)

	got, err := ApplyDiff([]byte(reBase), []byte(diff))
	if err != nil {
		t.Fatalf("ApplyDiff: %v", err)
	}
	if string(got) != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}
