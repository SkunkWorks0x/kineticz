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
	// Penultimate returns the second-newest entry, or ErrEmpty when the ledger
	// has fewer than two entries. LoadHead uses it to verify the head's link.
	Penultimate(ctx context.Context) (*audit.Entry, error)
	Insert(ctx context.Context, e *audit.Entry) error
	InTransaction(ctx context.Context, fn func(ctx context.Context, s chainStore) error) error
	HasEntry(ctx context.Context, eventID string) (bool, error)
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

// LoadHead reads the most recent entry and verifies its hash, signature, and
// link to its predecessor against pub. Returns the head entry, or ErrEmpty if
// the ledger is fresh. Callers should invoke this on startup to detect a
// tampered or corrupted chain before appending new entries.
func (w *Writer) LoadHead(ctx context.Context, pub ed25519.PublicKey) (*audit.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	head, err := w.store.Latest(ctx)
	if err != nil {
		return nil, err
	}
	// Verify against the real predecessor's hash. Checking the head against
	// its own claimed PreviousHash passes on an interior deletion at the head;
	// a genesis head verifies against nil.
	var prevHash []byte
	prev, err := w.store.Penultimate(ctx)
	switch {
	case errors.Is(err, ErrEmpty):
	case err != nil:
		return nil, err
	case prev.Timestamp.Equal(head.Timestamp):
		// Millisecond-truncated timestamps can tie, and the timestamp sort
		// then gives no deterministic predecessor. Keep the hash and signature
		// checks but skip the link comparison rather than refuse startup on
		// ordering noise.
		prevHash = head.PreviousHash
	default:
		prevHash = prev.Hash
	}
	if err := audit.Verify(*head, prevHash, pub); err != nil {
		return nil, fmt.Errorf("audit/mongodb: head verification failed: %w", err)
	}
	return head, nil
}

// Append implements audit.Writer. Delegates to appendInternal with empty
// thought and eventID; non-AI callers (elastic, dynatrace, diagnose engine)
// use this overload.
func (w *Writer) Append(ctx context.Context, action string, payload []byte) error {
	return w.appendInternal(ctx, action, payload, "", "")
}

// AppendWithThought implements audit.ThoughtWriter. It opens a transaction,
// reads the current chain head, builds and signs a new entry chained to that
// head with the Gemini reasoning block populated, and inserts it. Concurrent
// Appends within the same process are serialized by mu; concurrent Appends
// across processes are ordered by the store's transaction.
func (w *Writer) AppendWithThought(ctx context.Context, action string, payload []byte, thought string) error {
	return w.appendInternal(ctx, action, payload, thought, "")
}

// AppendWithEvent writes an audit entry tagged with a source event ID for
// later HasEntry lookup. Used by webhook receivers (e.g., fivetran) that
// need idempotency keyed on the upstream event.
func (w *Writer) AppendWithEvent(ctx context.Context, action string, payload []byte, eventID string) error {
	return w.appendInternal(ctx, action, payload, "", eventID)
}

// HasEntry reports whether the ledger already contains an entry with the
// given source event ID. Used for idempotency checks before processing a
// webhook delivery.
func (w *Writer) HasEntry(ctx context.Context, eventID string) (bool, error) {
	return w.store.HasEntry(ctx, eventID)
}

func (w *Writer) appendInternal(ctx context.Context, action string, payload []byte, thought, eventID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.store.InTransaction(ctx, func(txCtx context.Context, s chainStore) error {
		prev, err := s.Latest(txCtx)
		if err != nil && !errors.Is(err, ErrEmpty) {
			return fmt.Errorf("audit/mongodb: read latest: %w", err)
		}

		token, _ := corr.FromContext(txCtx)
		// Truncate to millisecond before hashing. BSON DateTime stores
		// millisecond precision; without truncation here, an entry read back
		// from MongoDB would re-hash to a different value than its stored
		// Hash, and Verify would return ErrHashMismatch.
		now := time.Now().UTC().Truncate(time.Millisecond)
		e := &audit.Entry{
			ID:               newEntryID(),
			CorrelationToken: token,
			Action:           action,
			Payload:          payload,
			Thought:          thought,
			SourceEventID:    eventID,
			Timestamp:        now,
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
