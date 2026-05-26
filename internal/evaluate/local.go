package evaluate

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
)

// Verdict is the outcome of a single Evaluate run.
type Verdict int

const (
	VerdictAllow Verdict = iota
	VerdictBlock
)

// LocalResult is the local pre-filter's decision plus a stable reason
// identifier suitable for audit payloads.
type LocalResult struct {
	Verdict Verdict
	Reason  string
}

// RunLocal applies the local pre-filter checks in order. Returns the first
// BLOCK encountered, or ALLOW if all checks pass. Pure function; no I/O.
//
// Checks:
//  1. patched bytes must parse as Go (parse error → BLOCK).
//  2. all exported function signatures in orig must remain unchanged in
//     patched (removed or modified signature → BLOCK).
func RunLocal(orig, patched []byte) LocalResult {
	if r := parsesAsGo(patched); r.Verdict == VerdictBlock {
		return r
	}
	return preservesSignature(orig, patched)
}

func parsesAsGo(src []byte) LocalResult {
	_, err := parser.ParseFile(token.NewFileSet(), "patched.go", src, parser.AllErrors)
	if err != nil {
		return LocalResult{Verdict: VerdictBlock, Reason: "parse_error: " + err.Error()}
	}
	return LocalResult{Verdict: VerdictAllow}
}

func preservesSignature(orig, patched []byte) LocalResult {
	origSigs, err := extractExportedSigs(orig)
	if err != nil {
		// Original failed to parse — caller probably gave us malformed orig.
		// Skip this check (treat as ALLOW); ParsesAsGo already validated patched.
		return LocalResult{Verdict: VerdictAllow}
	}
	newSigs, err := extractExportedSigs(patched)
	if err != nil {
		return LocalResult{Verdict: VerdictBlock, Reason: "patched_parse_error: " + err.Error()}
	}
	for name, oldSig := range origSigs {
		newSig, ok := newSigs[name]
		if !ok {
			return LocalResult{Verdict: VerdictBlock, Reason: "removed_exported_sig: " + name}
		}
		if oldSig != newSig {
			return LocalResult{Verdict: VerdictBlock, Reason: fmt.Sprintf("changed_exported_sig: %s (was %s, now %s)", name, oldSig, newSig)}
		}
	}
	return LocalResult{Verdict: VerdictAllow}
}

func extractExportedSigs(src []byte) (map[string]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "src.go", src, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if !ast.IsExported(fn.Name.Name) {
			continue
		}
		key := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) > 0 {
			key = renderType(fset, fn.Recv.List[0].Type) + "." + key
		}
		out[key] = renderFuncSig(fset, fn.Type)
	}
	return out, nil
}

func renderFuncSig(fset *token.FileSet, ft *ast.FuncType) string {
	var b bytes.Buffer
	b.WriteString("(")
	if ft.Params != nil {
		for i, field := range ft.Params.List {
			if i > 0 {
				b.WriteString(", ")
			}
			typ := renderType(fset, field.Type)
			// A field with N names is N parameters of the same type.
			n := len(field.Names)
			if n == 0 {
				n = 1
			}
			for j := 0; j < n; j++ {
				if j > 0 {
					b.WriteString(", ")
				}
				b.WriteString(typ)
			}
		}
	}
	b.WriteString(") (")
	if ft.Results != nil {
		for i, field := range ft.Results.List {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(renderType(fset, field.Type))
		}
	}
	b.WriteString(")")
	return b.String()
}

func renderType(fset *token.FileSet, e ast.Expr) string {
	var b bytes.Buffer
	_ = printer.Fprint(&b, fset, e)
	return b.String()
}
