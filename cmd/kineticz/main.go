package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/skunkworks0x/kineticz/internal/arize"
	"github.com/skunkworks0x/kineticz/internal/audit"
	auditmongo "github.com/skunkworks0x/kineticz/internal/audit/mongodb"
	"github.com/skunkworks0x/kineticz/internal/commit"
	"github.com/skunkworks0x/kineticz/internal/dynatrace"
	"github.com/skunkworks0x/kineticz/internal/elastic"
	"github.com/skunkworks0x/kineticz/internal/engine/diagnose"
	"github.com/skunkworks0x/kineticz/internal/evaluate"
	"github.com/skunkworks0x/kineticz/internal/fivetran"
	"github.com/skunkworks0x/kineticz/internal/gemini"
	"github.com/skunkworks0x/kineticz/internal/gitlab"
	"github.com/skunkworks0x/kineticz/internal/repair"
)

const (
	shutdownTimeout = 30 * time.Second
	metaCollection  = "kineticz_meta"
	signingKeyDocID = "signing_key"
)

// Deps is the wired set of orchestration components consumed by WireHandler.
// Production main() builds this from env vars; integration tests inject mocks.
type Deps struct {
	EventStore     fivetran.EventStore
	Audit          audit.Writer
	Diagnose       *diagnose.Engine
	Repair         *repair.Coordinator
	Evaluate       *evaluate.Gate
	Commit         *commit.Coordinator
	Target         repair.TargetReader
	PublicKey      ed25519.PublicKey
	ProjectID      string
	TargetBranch   string
	FivetranSecret string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kineticz: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	logStartup(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	deps, cleanup, err := buildDeps(ctx, cfg)
	if err != nil {
		return fmt.Errorf("wiring: %w", err)
	}
	defer cleanup()

	handler := WireHandler(deps)
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("listening on :%s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return fmt.Errorf("server: %w", err)
	case <-ctx.Done():
		log.Println("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	log.Println("kineticz stopped cleanly")
	return nil
}

// WireHandler builds the http.Handler from a populated Deps.
func WireHandler(d Deps) http.Handler {
	pipeline := func(ctx context.Context, anomaly fivetran.Anomaly) {
		runPipeline(ctx, d, anomaly)
	}
	rec := fivetran.NewReceiver(d.EventStore, d.FivetranSecret, pipeline)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/audit/pubkey", pubkeyHandler(d.PublicKey))
	mux.Handle("/webhooks/fivetran", rec)
	return mux
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// pubkeyHandler serves the base64-encoded Ed25519 public key so judges can
// independently verify the audit chain against entries in MongoDB.
func pubkeyHandler(pub ed25519.PublicKey) http.HandlerFunc {
	encoded := base64.StdEncoding.EncodeToString(pub)
	body := fmt.Sprintf(`{"algorithm":"ed25519","public_key":"%s"}`, encoded)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}
}

func runPipeline(ctx context.Context, d Deps, anomaly fivetran.Anomaly) {
	fail := func(stage string, err error) {
		payload, _ := json.Marshal(map[string]any{
			"event_id": anomaly.EventID,
			"stage":    stage,
			"error":    err.Error(),
		})
		_ = d.Audit.Append(ctx, "PIPELINE_FAILED", payload)
	}

	columns := make([]string, 0, len(anomaly.ColumnChanges))
	for _, ch := range anomaly.ColumnChanges {
		columns = append(columns, ch.Column)
	}
	contractName := anomaly.SchemaName + "/" + anomaly.TableName
	syncEndMs := time.Now().UnixMilli()
	syncStartMs := anomaly.Timestamp.UnixMilli()

	diag, err := d.Diagnose.Diagnose(ctx, elastic.ContractQuery{
		ContractName:  contractName,
		Columns:       columns,
		DiffEmbedding: nil,
	}, syncStartMs, syncEndMs)
	if err != nil {
		fail("diagnose", err)
		return
	}
	if err := diag.Validate(); err != nil {
		fail("validate", err)
		return
	}

	targetPath := filepath.Join("internal", "pipeline", anomaly.TableName+".go")

	repairRes, err := d.Repair.Repair(ctx, diag, targetPath)
	if err != nil {
		fail("repair", err)
		return
	}

	orig, err := d.Target.Read(ctx, targetPath)
	if err != nil {
		fail("target_read", err)
		return
	}

	patched, err := commit.ApplyDiff(orig, repairRes.PatchDiff)
	if err != nil {
		fail("apply_diff", err)
		return
	}

	evalRes, err := d.Evaluate.Evaluate(ctx, orig, patched, repairRes.PatchDiff)
	if err != nil {
		fail("evaluate", err)
		return
	}
	if evalRes.Verdict != evaluate.VerdictAllow {
		fail("evaluate_block", fmt.Errorf("verdict=BLOCK local_reason=%q", evalRes.LocalReason))
		return
	}

	mr, err := d.Commit.ApplyAndOpenMR(ctx, commit.Request{
		ProjectID:     d.ProjectID,
		TargetBranch:  d.TargetBranch,
		FilePath:      targetPath,
		FileContent:   patched,
		CommitMessage: "Kineticz auto-patch: " + contractName,
		MRTitle:       "Auto-patch " + anomaly.TableName + " schema drift",
		MRDescription: fmt.Sprintf("Anomaly %s triggered by upstream schema change.\n\nDiff:\n```\n%s\n```\n", anomaly.EventID, repairRes.PatchDiff),
	})
	if err != nil {
		fail("commit", err)
		return
	}

	payload, _ := json.Marshal(map[string]any{
		"event_id":   anomaly.EventID,
		"mr_iid":     mr.MRIID,
		"mr_url":     mr.MRURL,
		"commit_sha": mr.CommitSHA,
		"branch":     mr.Branch,
	})
	_ = d.Audit.Append(ctx, "PIPELINE_COMPLETE", payload)
}

// fsReader is a TargetReader rooted at a local directory. Production should
// substitute a GitLab-backed reader; this implementation is for demo runs
// where the source repository is checked out locally.
type fsReader struct {
	root string
}

func (f *fsReader) Read(_ context.Context, path string) ([]byte, error) {
	return os.ReadFile(filepath.Join(f.root, path))
}

type config struct {
	Port               string
	MongoURI           string
	MongoDB            string
	GeminiProjectID    string
	GeminiLocation     string
	GeminiModel        string
	GitLabURL          string
	GitLabToken        string
	GitLabProjectID    string
	GitLabTargetBranch string
	ArizeURL           string
	ArizeAPIKey        string
	ArizeRubricID      string
	ElasticURL         string
	DynatraceURL       string
	DynatraceToken     string
	FivetranSecret     string
	Ed25519SeedHex     string
	TargetRoot         string
}

func loadConfig() (config, error) {
	cfg := config{
		Port:               getenv("PORT", "8080"),
		MongoURI:           os.Getenv("MONGO_URI"),
		MongoDB:            getenv("MONGO_DB", "kineticz"),
		GeminiProjectID:    os.Getenv("GEMINI_PROJECT_ID"),
		GeminiLocation:     getenv("GEMINI_LOCATION", "us-central1"),
		GeminiModel:        getenv("GEMINI_MODEL", "gemini-3.5-flash"),
		GitLabURL:          os.Getenv("GITLAB_URL"),
		GitLabToken:        os.Getenv("GITLAB_TOKEN"),
		GitLabProjectID:    os.Getenv("GITLAB_PROJECT_ID"),
		GitLabTargetBranch: getenv("GITLAB_TARGET_BRANCH", "main"),
		ArizeURL:           os.Getenv("ARIZE_URL"),
		ArizeAPIKey:        os.Getenv("ARIZE_API_KEY"),
		ArizeRubricID:      getenv("ARIZE_RUBRIC_ID", "kineticz-patch-rubric"),
		ElasticURL:         os.Getenv("ELASTIC_URL"),
		DynatraceURL:       os.Getenv("DYNATRACE_URL"),
		DynatraceToken:     os.Getenv("DYNATRACE_TOKEN"),
		FivetranSecret:     os.Getenv("FIVETRAN_SECRET"),
		Ed25519SeedHex:     os.Getenv("KINETICZ_ED25519_SEED"),
		TargetRoot:         getenv("TARGET_ROOT", "."),
	}

	missing := []string{}
	for k, v := range map[string]string{
		"MONGO_URI":             cfg.MongoURI,
		"GEMINI_PROJECT_ID":     cfg.GeminiProjectID,
		"GITLAB_URL":            cfg.GitLabURL,
		"GITLAB_TOKEN":          cfg.GitLabToken,
		"GITLAB_PROJECT_ID":     cfg.GitLabProjectID,
		"ARIZE_URL":             cfg.ArizeURL,
		"ARIZE_API_KEY":         cfg.ArizeAPIKey,
		"ELASTIC_URL":           cfg.ElasticURL,
		"DYNATRACE_URL":         cfg.DynatraceURL,
		"FIVETRAN_SECRET":       cfg.FivetranSecret,
		"KINETICZ_ED25519_SEED": cfg.Ed25519SeedHex,
	} {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing required env vars: %v", missing)
	}
	if len(cfg.Ed25519SeedHex) != hex.EncodedLen(ed25519.SeedSize) {
		return cfg, fmt.Errorf("KINETICZ_ED25519_SEED must be %d hex characters (32 bytes)", hex.EncodedLen(ed25519.SeedSize))
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func logStartup(cfg config) {
	log.Println("--- kineticz startup ---")
	log.Printf("  port=%s mongo_db=%s", cfg.Port, cfg.MongoDB)
	log.Printf("  gemini=%s/%s model=%s", cfg.GeminiProjectID, cfg.GeminiLocation, cfg.GeminiModel)
	log.Printf("  gitlab=%s project=%s target_branch=%s", cfg.GitLabURL, cfg.GitLabProjectID, cfg.GitLabTargetBranch)
	log.Printf("  arize=%s rubric=%s", cfg.ArizeURL, cfg.ArizeRubricID)
	log.Printf("  elastic=%s dynatrace=%s", cfg.ElasticURL, cfg.DynatraceURL)
	log.Printf("  target_root=%s", cfg.TargetRoot)
	log.Println("------------------------")
}

func buildDeps(ctx context.Context, cfg config) (Deps, func(), error) {
	mongoClient, err := mongo.Connect(options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return Deps{}, nil, fmt.Errorf("mongo connect: %w", err)
	}
	cleanup := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mongoClient.Disconnect(shutdownCtx)
	}

	seed, err := hex.DecodeString(cfg.Ed25519SeedHex)
	if err != nil {
		return Deps{}, cleanup, fmt.Errorf("decode KINETICZ_ED25519_SEED: %w", err)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)

	if err := persistPublicKey(ctx, mongoClient, cfg.MongoDB, pub); err != nil {
		return Deps{}, cleanup, fmt.Errorf("persist public key: %w", err)
	}

	writer, err := auditmongo.NewMongoWriter(ctx, mongoClient, cfg.MongoDB, priv)
	if err != nil {
		return Deps{}, cleanup, fmt.Errorf("audit writer: %w", err)
	}

	// Verify the chain head against the seed-derived public key. A mismatch
	// here means either the seed changed or the ledger was tampered with;
	// either way, refusing to start is the correct response.
	if _, err := writer.LoadHead(ctx, pub); err != nil && !errors.Is(err, auditmongo.ErrEmpty) {
		return Deps{}, cleanup, fmt.Errorf("audit chain head verification failed: %w", err)
	}

	httpDefault := http.DefaultClient
	elasticClient := elastic.NewClient(httpDefault, writer, cfg.ElasticURL)
	dynatraceClient := dynatrace.NewClient(httpDefault, writer, cfg.DynatraceURL)
	arizeClient := arize.NewHTTPClient(httpDefault, cfg.ArizeURL, cfg.ArizeAPIKey, cfg.ArizeRubricID)
	gitlabClient := gitlab.NewHTTPClient(httpDefault, cfg.GitLabURL, cfg.GitLabToken)
	geminiClient := gemini.NewVertexClient(httpDefault, writer, cfg.GeminiProjectID, cfg.GeminiLocation, cfg.GeminiModel, envTokenFunc)

	target := &fsReader{root: cfg.TargetRoot}

	diagnoseEngine := diagnose.New(elasticClient, dynatraceClient, writer)
	repairCoord := repair.New(geminiClient, writer, target)
	evalGate := evaluate.New(arizeClient, writer, noopIndexer{})
	commitCoord := commit.New(gitlabClient, writer)

	return Deps{
		EventStore:     writer,
		Audit:          writer,
		Diagnose:       diagnoseEngine,
		Repair:         repairCoord,
		Evaluate:       evalGate,
		Commit:         commitCoord,
		Target:         target,
		PublicKey:      pub,
		ProjectID:      cfg.GitLabProjectID,
		TargetBranch:   cfg.GitLabTargetBranch,
		FivetranSecret: cfg.FivetranSecret,
	}, cleanup, nil
}

// persistPublicKey upserts the running public key into MongoDB so external
// verifiers can fetch it without trusting the running process. The /audit/pubkey
// endpoint serves the same value from memory.
func persistPublicKey(ctx context.Context, client *mongo.Client, dbName string, pub ed25519.PublicKey) error {
	coll := client.Database(dbName).Collection(metaCollection)
	doc := bson.D{
		{Key: "_id", Value: signingKeyDocID},
		{Key: "algorithm", Value: "ed25519"},
		{Key: "public_key", Value: base64.StdEncoding.EncodeToString(pub)},
		{Key: "rotated_at", Value: time.Now().UTC()},
	}
	_, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: signingKeyDocID}}, doc, options.Replace().SetUpsert(true))
	return err
}

// envTokenFunc reads a Google Cloud access token from GOOGLE_ACCESS_TOKEN.
// For Cloud Run, replace with a metadata-server fetcher that refreshes
// short-lived tokens.
func envTokenFunc(_ context.Context) (string, error) {
	t := os.Getenv("GOOGLE_ACCESS_TOKEN")
	if t == "" {
		return "", fmt.Errorf("gemini: GOOGLE_ACCESS_TOKEN not set")
	}
	return t, nil
}

// noopIndexer is the default evaluate.RejectedIndexer when production
// hasn't wired an Elastic-backed indexer yet. Drops rejected diffs on the
// floor. Replace before final hackathon submission.
type noopIndexer struct{}

func (noopIndexer) Index(_ context.Context, _ string, _ []byte) error {
	return nil
}
