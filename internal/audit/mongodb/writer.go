package mongodb

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
)

// CollectionName is the MongoDB collection that stores audit entries.
const CollectionName = "audit_ledger"

// ErrEmpty is returned by chainStore.Latest when the ledger has no entries.
var ErrEmpty = errors.New("audit/mongodb: chain is empty")

// chainStore abstracts the transactional storage needed by Writer. The
// production implementation (mongoStore) is backed by mongo-driver v2; tests
// substitute an in-memory store that preserves the same atomic semantics.
type chainStore interface {
	Latest(ctx context.Context) (*audit.Entry, error)
	Insert(ctx context.Context, e *audit.Entry) error
	InTransaction(ctx context.Context, fn func(ctx context.Context, s chainStore) error) error
}

// Writer is a concrete audit.Writer backed by a transactional chain store.
// Append serializes within-process via mu and atomically chains across
// processes via the store's transaction.
type Writer struct {
	store chainStore
	priv  ed25519.PrivateKey
	mu    sync.Mutex
}

// NewWriter constructs a Writer over the given store and signing key.
// LoadHead must be called before the first Append if the caller wants to
// verify the existing chain head; otherwise Append loads it lazily inside
// the first transaction.
func NewWriter(store chainStore, priv ed25519.PrivateKey) *Writer {
	return &Writer{store: store, priv: priv}
}

// LoadHead reads the most recent entry and verifies its signature against pub.
// Returns the head entry, or ErrEmpty if the ledger is fresh. Callers should
// invoke this on startup to detect a tampered or corrupted chain before
// appending new entries.
func (w *Writer) LoadHead(ctx context.Context, pub ed25519.PublicKey) (*audit.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	head, err := w.store.Latest(ctx)
	if err != nil {
		return nil, err
	}
	if err := audit.Verify(*head, head.PreviousHash, pub); err != nil {
		return nil, fmt.Errorf("audit/mongodb: head verification failed: %w", err)
	}
	return head, nil
}

// Append implements audit.Writer. It opens a transaction, reads the current
// chain head, builds and signs a new entry chained to that head, and inserts
// it. Concurrent Appends within the same process are serialized by mu;
// concurrent Appends across processes are ordered by the store's transaction.
func (w *Writer) Append(ctx context.Context, action string, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.store.InTransaction(ctx, func(txCtx context.Context, s chainStore) error {
		prev, err := s.Latest(txCtx)
		if err != nil && !errors.Is(err, ErrEmpty) {
			return fmt.Errorf("audit/mongodb: read latest: %w", err)
		}

		token, _ := corr.FromContext(txCtx)
		e := &audit.Entry{
			ID:               newEntryID(),
			CorrelationToken: token,
			Action:           action,
			Payload:          payload,
			Timestamp:        time.Now().UTC(),
		}
		if prev != nil {
			e.PreviousHash = prev.Hash
		}
		audit.Chain(e, w.priv)
		return s.Insert(txCtx, e)
	})
}

func newEntryID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
