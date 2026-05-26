package audit

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/skunkworks0x/kineticz/internal/corr"
)

type Entry struct {
	ID               string
	CorrelationToken corr.CorrelationToken
	Action           string
	Payload          []byte
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

func computeHash(e *Entry) []byte {
	h := sha256.New()
	h.Write(e.PreviousHash)
	h.Write([]byte(e.Action))
	h.Write(e.Payload)
	h.Write([]byte(e.Timestamp.UTC().Format(time.RFC3339Nano)))
	return h.Sum(nil)
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
