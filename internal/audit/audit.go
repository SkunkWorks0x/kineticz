package audit

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/skunkworks0x/kineticz/internal/corr"
)

type Entry struct {
	ID               string
	CorrelationToken corr.CorrelationToken
	Action           string
	Payload          []byte
	Thought          string
	// SourceEventID is metadata for idempotency lookups (e.g., Fivetran event
	// IDs). EXCLUDED from CanonicalBytes so adding it does not invalidate
	// stored hashes. Tamper-evident event IDs should live inside Payload.
	SourceEventID    string
	PreviousHash     []byte
	Hash             []byte
	Ed25519Signature []byte
	Timestamp        time.Time
}

var (
	ErrBrokenChain  = errors.New("audit: broken chain link")
	ErrHashMismatch = errors.New("audit: hash mismatch")
	ErrBadSignature = errors.New("audit: invalid signature")
)

// CanonicalFields documents the field order used by CanonicalBytes and thus
// the SHA-256 hash. Reordering invalidates every stored hash and signature.
var CanonicalFields = []string{"PreviousHash", "Action", "Payload", "Thought", "Timestamp"}

// CanonicalBytes returns the deterministic byte representation hashed by
// computeHash. Fields are written in CanonicalFields order with 8-byte
// big-endian length prefixes; length-prefixing prevents adjacent-field
// boundary collisions (e.g., Action="foo" Payload="bar" cannot hash-collide
// with Action="foob" Payload="ar").
func (e *Entry) CanonicalBytes() []byte {
	var buf bytes.Buffer
	writeLengthPrefixed(&buf, e.PreviousHash)
	writeLengthPrefixed(&buf, []byte(e.Action))
	writeLengthPrefixed(&buf, e.Payload)
	writeLengthPrefixed(&buf, []byte(e.Thought))
	writeLengthPrefixed(&buf, []byte(e.Timestamp.UTC().Format(time.RFC3339Nano)))
	return buf.Bytes()
}

func writeLengthPrefixed(w io.Writer, b []byte) {
	var sz [8]byte
	binary.BigEndian.PutUint64(sz[:], uint64(len(b)))
	_, _ = w.Write(sz[:])
	_, _ = w.Write(b)
}

func computeHash(e *Entry) []byte {
	sum := sha256.Sum256(e.CanonicalBytes())
	return sum[:]
}

func Chain(e *Entry, priv ed25519.PrivateKey) {
	e.Hash = computeHash(e)
	e.Ed25519Signature = ed25519.Sign(priv, e.Hash)
}

func Verify(e Entry, prevHash []byte, pub ed25519.PublicKey) error {
	if !bytes.Equal(e.PreviousHash, prevHash) {
		return fmt.Errorf("%w: entry PreviousHash does not match previous Hash", ErrBrokenChain)
	}
	if !bytes.Equal(computeHash(&e), e.Hash) {
		return fmt.Errorf("%w: recomputed SHA-256 differs from stored Hash", ErrHashMismatch)
	}
	if !ed25519.Verify(pub, e.Hash, e.Ed25519Signature) {
		return fmt.Errorf("%w: ed25519.Verify returned false", ErrBadSignature)
	}
	return nil
}
