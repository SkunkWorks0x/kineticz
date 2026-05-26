package mongodb

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
)

// NewMongoWriter wires a Writer to a MongoDB Atlas cluster. It creates the
// audit_ledger collection's required indexes idempotently and returns a
// Writer ready for Append. Pass the Ed25519 private key that will sign every
// chained entry.
func NewMongoWriter(ctx context.Context, client *mongo.Client, dbName string, priv ed25519.PrivateKey) (*Writer, error) {
	coll := client.Database(dbName).Collection(CollectionName)
	uniqueOnEventID := options.Index().
		SetUnique(true).
		SetPartialFilterExpression(bson.D{{Key: "source_event_id", Value: bson.D{{Key: "$exists", Value: true}}}})
	if _, err := coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "correlation_token", Value: 1}}},
		{Keys: bson.D{{Key: "timestamp", Value: -1}}},
		// Unique partial index enforces atomic idempotency: concurrent webhook
		// deliveries with the same source_event_id race in the database, the
		// loser receives a duplicate-key error, and Insert wraps that as
		// audit.ErrDuplicateEvent for the fivetran receiver to detect.
		{Keys: bson.D{{Key: "source_event_id", Value: 1}}, Options: uniqueOnEventID},
	}); err != nil {
		return nil, fmt.Errorf("audit/mongodb: create indexes: %w", err)
	}
	return NewWriter(&mongoStore{client: client, coll: coll}, priv), nil
}

type mongoStore struct {
	client *mongo.Client
	coll   *mongo.Collection
}

type entryDoc struct {
	ID               string    `bson:"id"`
	CorrelationToken string    `bson:"correlation_token"`
	Action           string    `bson:"action"`
	Payload          []byte    `bson:"payload"`
	Thought          string    `bson:"thought"`
	SourceEventID    string    `bson:"source_event_id,omitempty"`
	PreviousHash     []byte    `bson:"previous_hash"`
	Hash             []byte    `bson:"hash"`
	Ed25519Signature []byte    `bson:"ed25519_signature"`
	Timestamp        time.Time `bson:"timestamp"`
}

func (d *entryDoc) toEntry() *audit.Entry {
	return &audit.Entry{
		ID:               d.ID,
		CorrelationToken: corr.CorrelationToken(d.CorrelationToken),
		Action:           d.Action,
		Payload:          d.Payload,
		Thought:          d.Thought,
		SourceEventID:    d.SourceEventID,
		PreviousHash:     d.PreviousHash,
		Hash:             d.Hash,
		Ed25519Signature: d.Ed25519Signature,
		Timestamp:        d.Timestamp,
	}
}

func entryToDoc(e *audit.Entry) entryDoc {
	return entryDoc{
		ID:               e.ID,
		CorrelationToken: string(e.CorrelationToken),
		Action:           e.Action,
		Payload:          e.Payload,
		Thought:          e.Thought,
		SourceEventID:    e.SourceEventID,
		PreviousHash:     e.PreviousHash,
		Hash:             e.Hash,
		Ed25519Signature: e.Ed25519Signature,
		Timestamp:        e.Timestamp,
	}
}

func (m *mongoStore) Latest(ctx context.Context) (*audit.Entry, error) {
	opts := options.FindOne().SetSort(bson.D{{Key: "timestamp", Value: -1}})
	var doc entryDoc
	err := m.coll.FindOne(ctx, bson.D{}, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrEmpty
	}
	if err != nil {
		return nil, fmt.Errorf("audit/mongodb: find latest: %w", err)
	}
	return doc.toEntry(), nil
}

func (m *mongoStore) Insert(ctx context.Context, e *audit.Entry) error {
	if _, err := m.coll.InsertOne(ctx, entryToDoc(e)); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("%w: %v", audit.ErrDuplicateEvent, err)
		}
		return fmt.Errorf("audit/mongodb: insert: %w", err)
	}
	return nil
}

func (m *mongoStore) HasEntry(ctx context.Context, eventID string) (bool, error) {
	if eventID == "" {
		return false, nil
	}
	// Filter on source_event_id AND action so a planted document with the
	// wrong action cannot trick the idempotency check.
	filter := bson.D{
		{Key: "source_event_id", Value: eventID},
		{Key: "action", Value: "FIVETRAN_RECEIVED"},
	}
	opts := options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})
	err := m.coll.FindOne(ctx, filter, opts).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("audit/mongodb: HasEntry lookup: %w", err)
	}
	return true, nil
}

func (m *mongoStore) InTransaction(ctx context.Context, fn func(ctx context.Context, s chainStore) error) error {
	sess, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("audit/mongodb: start session: %w", err)
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(sessCtx context.Context) (any, error) {
		return nil, fn(sessCtx, m)
	})
	if err != nil {
		return fmt.Errorf("audit/mongodb: transaction: %w", err)
	}
	return nil
}
