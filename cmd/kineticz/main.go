package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/skunkworks0x/kineticz/internal/arize"
	"github.com/skunkworks0x/kineticz/internal/audit"
	auditmongo "github.com/skunkworks0x/kineticz/internal/audit/mongodb"
	"github.com/skunkworks0x/kineticz/internal/commit"
	"github.com/skunkworks0x/kineticz/internal/corr"
	"github.com/skunkworks0x/kineticz/internal/dynatrace"
	"github.com/skunkworks0x/kineticz/internal/elastic"
	"github.com/skunkworks0x/kineticz/internal/engine/diagnose"
	"github.com/skunkworks0x/kineticz/internal/evaluate"
	"github.com/skunkworks0x/kineticz/internal/fivetran"
	"github.com/skunkworks0x/kineticz/internal/gemini"
	"github.com/skunkworks0x/kineticz/internal/gitlab"
	"github.com/skunkworks0x/kineticz/internal/phoenix"
	"github.com/skunkworks0x/kineticz/internal/repair"
)

const (
	shutdownTimeout = 30 * time.Second
	metaCollection  = "kineticz_meta"
	signingKeyDocID = "signing_key"

	// phoenix-mcp runs as a node subprocess. These paths match the runtime
	// image: the distroless nodejs22 node binary and the baked phoenix-mcp
	// entrypoint (see Dockerfile).
	phoenixNodeBin = "/nodejs/bin/node"
	phoenixEntry   = "/app/node_modules/@arizeai/phoenix-mcp/build/index.js"
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
	PublicKey      ed25519.PublicKey
	ProjectID      string
	TargetBranch   string
	FivetranSecret string
	TraceFlush     func(context.Context) error
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if len(os.Args) > 1 && os.Args[1] == "verify" {
		if err := runVerify(context.Background()); err != nil {
			slog.Error("verify failed", "error", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		slog.Error("kineticz failed", "error", err)
		os.Exit(1)
	}
}

// runVerify walks the full audit chain and reports linkage, hash, and
// signature results. It needs MONGO_URI, MONGO_DB (default kineticz), and
// KINETICZ_ED25519_SEED; the server env set is not required. Exit 0 = intact.
// Tail truncation is outside the walk: compare the printed entry count against
// an external anchor to detect it.
func runVerify(ctx context.Context) error {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		return errors.New("verify: MONGO_URI is required")
	}
	dbName := getenv("MONGO_DB", "kineticz")
	seedHex := os.Getenv("KINETICZ_ED25519_SEED")
	if len(seedHex) != hex.EncodedLen(ed25519.SeedSize) {
		return fmt.Errorf("verify: KINETICZ_ED25519_SEED must be %d hex characters (32 bytes)", hex.EncodedLen(ed25519.SeedSize))
	}
	seed, err := hex.DecodeString(seedHex)
	if err != nil {
		return fmt.Errorf("verify: decode KINETICZ_ED25519_SEED: %w", err)
	}
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("verify: mongo connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	entries, err := auditmongo.ReadAllEntries(ctx, client, dbName)
	if err != nil {
		return fmt.Errorf("verify: read ledger: %w", err)
	}

	rep := audit.VerifyChain(entries, pub)
	if !rep.Valid {
		return fmt.Errorf("verify: chain INVALID at entry %d (id=%s): %s [%d entries walked]",
			rep.FailedIndex, rep.FailedID, rep.Reason, rep.Entries)
	}
	fmt.Printf("chain intact: %d entries; linkage, hashes, and signatures verified\n", rep.Entries)
	fmt.Println("tail truncation is outside this walk; compare the entry count against an external anchor")
	return nil
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
		slog.Info("http server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return fmt.Errorf("server: %w", err)
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	slog.Info("kineticz stopped cleanly")
	return nil
}

// WireHandler builds the http.Handler from a populated Deps.
func WireHandler(d Deps) http.Handler {
	pipeline := func(ctx context.Context, anomaly fivetran.Anomaly) {
		runPipeline(ctx, d, anomaly)
	}
	rec := fivetran.NewReceiver(d.EventStore, d.FivetranSecret, pipeline, d.TraceFlush)

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

// pubkeyHandler serves the hex-encoded Ed25519 public key so judges can
// independently verify the audit chain against entries in MongoDB. Hex
// matches the format of KINETICZ_ED25519_SEED, so the same toolchain
// (openssl, xxd) works for both halves of the keypair.
func pubkeyHandler(pub ed25519.PublicKey) http.HandlerFunc {
	encoded := hex.EncodeToString(pub)
	body := fmt.Sprintf(`{"algorithm":"ed25519","encoding":"hex","public_key":"%s"}`, encoded)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}
}

var connectorNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// validConnectorName bounds connector_name to a safe charset so it cannot
// traverse out of internal/pipeline/ when it forms the commit target path.
func validConnectorName(name string) error {
	if !connectorNamePattern.MatchString(name) {
		return fmt.Errorf("connector_name %q must match [A-Za-z0-9_-]+", name)
	}
	return nil
}

func runPipeline(ctx context.Context, d Deps, anomaly fivetran.Anomaly) {
	eventID := anomaly.EventID()

	ctx, span := arize.Tracer().Start(ctx, "kineticz.pipeline")
	defer span.End()
	span.SetAttributes(
		attribute.String("openinference.span.kind", "CHAIN"),
		attribute.String("kineticz.event_id", eventID),
		attribute.String("kineticz.connector_id", anomaly.ConnectorID),
		attribute.String("kineticz.event_type", anomaly.Event),
	)
	if tok, ok := corr.FromContext(ctx); ok {
		span.SetAttributes(attribute.String("kineticz.correlation_token", string(tok)))
	}

	fail := func(stage string, err error) {
		span.SetStatus(codes.Error, stage+": "+err.Error())
		span.RecordError(err)
		payload, _ := json.Marshal(map[string]any{
			"event_id": eventID,
			"stage":    stage,
			"error":    err.Error(),
		})
		_ = d.Audit.Append(ctx, "PIPELINE_FAILED", payload)
	}

	if err := validConnectorName(anomaly.ConnectorName); err != nil {
		fail("validate_connector", err)
		return
	}

	// Real Fivetran webhooks don't carry column-level diffs. Diagnose runs
	// against the connector identity; richer column hints will come from
	// Dynatrace/Elastic side channels in a later phase.
	columns := []string{}
	contractName := anomaly.ConnectorType + "/" + anomaly.ConnectorName
	syncEndMs := time.Now().UnixMilli()
	syncStartMs := anomaly.Created.UnixMilli()

	diag, err := d.Diagnose.Diagnose(ctx, elastic.ContractQuery{
		ContractName: contractName,
		Columns:      columns,
	}, syncStartMs, syncEndMs)
	if err != nil {
		fail("diagnose", err)
		return
	}
	if err := diag.Validate(); err != nil {
		fail("validate", err)
		return
	}

	targetPath := filepath.Join("internal", "pipeline", anomaly.ConnectorName+".go")

	repairRes, err := d.Repair.Repair(ctx, diag, targetPath)
	if err != nil {
		fail("repair", err)
		return
	}

	evalRes, err := d.Evaluate.Evaluate(ctx, repairRes.Orig, repairRes.Patched, repairRes.PatchDiff)
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
		FileContent:   repairRes.Patched,
		CommitMessage: "Kineticz auto-patch: " + contractName,
		MRTitle:       "Auto-patch " + anomaly.ConnectorName + " schema drift",
		MRDescription: mrDescription(eventID, repairRes.PatchDiff, len(diag.PriorRepairs)),
	})
	if err != nil {
		fail("commit", err)
		return
	}

	payload, _ := json.Marshal(map[string]any{
		"event_id":   eventID,
		"mr_iid":     mr.MRIID,
		"mr_url":     mr.MRURL,
		"commit_sha": mr.CommitSHA,
		"branch":     mr.Branch,
	})
	_ = d.Audit.Append(ctx, "PIPELINE_COMPLETE", payload)
}

// mrDescription renders the MR body. The priors line surfaces the Phoenix MCP
// self-introspection in GitLab, where a reviewer sees it without opening the
// trace.
func mrDescription(eventID string, diff []byte, priorCount int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Anomaly %s triggered by upstream schema change.\n\n", eventID)
	if priorCount > 0 {
		fmt.Fprintf(&b, "Prior repair attempts considered: %d (read back from Arize Phoenix via MCP).\n\n", priorCount)
	}
	fmt.Fprintf(&b, "Diff:\n```\n%s\n```\n", diff)
	return b.String()
}

// gitlabFileReader implements repair.TargetReader against the GitLab
// repository-files API, reading the latest content of FilePath at TargetBranch.
// This replaces the demo-only fsReader so the orchestrator sees the same
// source-of-truth bytes that GitLab will accept on commit.
type gitlabFileReader struct {
	gl           gitlab.Client
	projectID    string
	targetBranch string
}

func (g *gitlabFileReader) Read(ctx context.Context, path string) ([]byte, error) {
	return g.gl.GetFileContent(ctx, g.projectID, path, g.targetBranch)
}

type config struct {
	Port                  string
	MongoURI              string
	MongoDB               string
	GeminiProjectID       string
	GeminiLocation        string
	GeminiModel           string
	GitLabURL             string
	GitLabToken           string
	GitLabProjectID       string
	GitLabTargetBranch    string
	PhoenixEndpoint       string
	PhoenixAPIKey         string
	PhoenixHost           string
	PhoenixProject        string
	ElasticURL            string
	ElasticAPIKey         string
	ElasticInferenceModel string
	DynatraceURL          string
	DynatraceToken        string
	FivetranSecret        string
	Ed25519SeedHex        string
}

func loadConfig() (config, error) {
	cfg := config{
		Port:                  getenv("PORT", "8080"),
		MongoURI:              os.Getenv("MONGO_URI"),
		MongoDB:               getenv("MONGO_DB", "kineticz"),
		GeminiProjectID:       os.Getenv("GEMINI_PROJECT_ID"),
		GeminiLocation:        getenv("GEMINI_LOCATION", "us-central1"),
		GeminiModel:           getenv("GEMINI_MODEL", "gemini-3.5-flash"),
		GitLabURL:             os.Getenv("GITLAB_URL"),
		GitLabToken:           os.Getenv("GITLAB_TOKEN"),
		GitLabProjectID:       os.Getenv("GITLAB_PROJECT_ID"),
		GitLabTargetBranch:    getenv("GITLAB_TARGET_BRANCH", "main"),
		PhoenixEndpoint:       os.Getenv("PHOENIX_COLLECTOR_ENDPOINT"),
		PhoenixAPIKey:         os.Getenv("PHOENIX_API_KEY"),
		PhoenixHost:           os.Getenv("PHOENIX_HOST"),
		PhoenixProject:        getenv("PHOENIX_PROJECT", "default"),
		ElasticURL:            os.Getenv("ELASTIC_URL"),
		ElasticAPIKey:         os.Getenv("ELASTIC_API_KEY"),
		ElasticInferenceModel: getenv("ELASTIC_INFERENCE_MODEL", ".multilingual-e5-small-elasticsearch"),
		DynatraceURL:          os.Getenv("DYNATRACE_URL"),
		DynatraceToken:        os.Getenv("DYNATRACE_TOKEN"),
		FivetranSecret:        os.Getenv("FIVETRAN_SECRET"),
		Ed25519SeedHex:        os.Getenv("KINETICZ_ED25519_SEED"),
	}

	missing := []string{}
	for k, v := range map[string]string{
		"MONGO_URI":                  cfg.MongoURI,
		"GEMINI_PROJECT_ID":          cfg.GeminiProjectID,
		"GITLAB_URL":                 cfg.GitLabURL,
		"GITLAB_TOKEN":               cfg.GitLabToken,
		"GITLAB_PROJECT_ID":          cfg.GitLabProjectID,
		"PHOENIX_COLLECTOR_ENDPOINT": cfg.PhoenixEndpoint,
		"PHOENIX_API_KEY":            cfg.PhoenixAPIKey,
		"ELASTIC_URL":                cfg.ElasticURL,
		"ELASTIC_API_KEY":            cfg.ElasticAPIKey,
		"DYNATRACE_URL":              cfg.DynatraceURL,
		"DYNATRACE_TOKEN":            cfg.DynatraceToken,
		"FIVETRAN_SECRET":            cfg.FivetranSecret,
		"KINETICZ_ED25519_SEED":      cfg.Ed25519SeedHex,
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
	slog.Info("kineticz starting",
		"port", cfg.Port,
		"mongo_db", cfg.MongoDB,
		"gemini_project", cfg.GeminiProjectID,
		"gemini_location", cfg.GeminiLocation,
		"gemini_model", cfg.GeminiModel,
		"gitlab_url", cfg.GitLabURL,
		"gitlab_project", cfg.GitLabProjectID,
		"gitlab_target_branch", cfg.GitLabTargetBranch,
		"phoenix_endpoint", cfg.PhoenixEndpoint,
		"phoenix_introspection", cfg.PhoenixHost != "",
		"elastic_url", cfg.ElasticURL,
		"dynatrace_url", cfg.DynatraceURL,
	)
}

func buildDeps(ctx context.Context, cfg config) (Deps, func(), error) {
	mongoClient, err := mongo.Connect(options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return Deps{}, nil, fmt.Errorf("mongo connect: %w", err)
	}
	tp, traceShutdown, err := arize.NewTracerProvider(ctx, cfg.PhoenixEndpoint, cfg.PhoenixAPIKey)
	if err != nil {
		_ = mongoClient.Disconnect(context.Background())
		return Deps{}, nil, fmt.Errorf("arize tracer provider: %w", err)
	}

	var phoenixClient phoenix.Client
	cleanup := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if phoenixClient != nil {
			_ = phoenixClient.Close()
		}
		_ = traceShutdown(shutdownCtx)
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
	elasticClient := elastic.NewClient(httpDefault, writer, cfg.ElasticURL, cfg.ElasticAPIKey, cfg.ElasticInferenceModel)
	dynatraceClient := dynatrace.NewClient(httpDefault, writer, cfg.DynatraceURL, cfg.DynatraceToken)
	gitlabClient := gitlab.NewHTTPClient(httpDefault, cfg.GitLabURL, cfg.GitLabToken)
	geminiClient := gemini.NewVertexClient(httpDefault, writer, cfg.GeminiProjectID, cfg.GeminiLocation, cfg.GeminiModel, envTokenFunc)

	target := &gitlabFileReader{
		gl:           gitlabClient,
		projectID:    cfg.GitLabProjectID,
		targetBranch: cfg.GitLabTargetBranch,
	}

	// Phoenix self-introspection is optional. With PHOENIX_HOST set, the
	// diagnose stage spawns the phoenix-mcp node subprocess and reads this
	// contract's prior repair traces. Unset leaves the Elastic+Dynatrace path
	// untouched.
	var diagOpts []diagnose.Option
	if cfg.PhoenixHost != "" {
		mcpEnv := append(os.Environ(),
			"PHOENIX_HOST="+cfg.PhoenixHost,
			"PHOENIX_API_KEY="+cfg.PhoenixAPIKey,
			"PHOENIX_PROJECT="+cfg.PhoenixProject,
		)
		phoenixClient = phoenix.New(phoenix.NodeDialer(phoenixNodeBin, phoenixEntry, mcpEnv), cfg.PhoenixProject)
		diagOpts = append(diagOpts, diagnose.WithPhoenix(phoenixClient, cfg.PhoenixProject))
	}

	diagnoseEngine := diagnose.New(elasticClient, dynatraceClient, writer, diagOpts...)
	repairCoord := repair.New(geminiClient, writer, target, commit.ApplyDiff)
	evalGate := evaluate.New(writer, noopIndexer{})
	commitCoord := commit.New(gitlabClient, writer)

	return Deps{
		EventStore:     writer,
		Audit:          writer,
		Diagnose:       diagnoseEngine,
		Repair:         repairCoord,
		Evaluate:       evalGate,
		Commit:         commitCoord,
		PublicKey:      pub,
		ProjectID:      cfg.GitLabProjectID,
		TargetBranch:   cfg.GitLabTargetBranch,
		FivetranSecret: cfg.FivetranSecret,
		TraceFlush:     tp.ForceFlush,
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

// envTokenFunc returns a Google Cloud access token. It prefers
// GOOGLE_ACCESS_TOKEN for local development and falls back to the Cloud Run
// metadata server, fetching a fresh short-lived token on each call.
func envTokenFunc(ctx context.Context) (string, error) {
	if t := os.Getenv("GOOGLE_ACCESS_TOKEN"); t != "" {
		return t, nil
	}

	const metadataURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL, nil)
	if err != nil {
		return "", fmt.Errorf("metadata server unreachable: %w", err)
	}
	req.Header.Set("Metadata-Flavor", "Google")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("metadata server unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server unreachable: status %d", resp.StatusCode)
	}

	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("metadata server unreachable: %w", err)
	}
	return body.AccessToken, nil
}

// noopIndexer is the default evaluate.RejectedIndexer when production
// hasn't wired an Elastic-backed indexer yet. Drops rejected diffs on the
// floor. Replace before final hackathon submission.
type noopIndexer struct{}

func (noopIndexer) Index(_ context.Context, _ string, _ []byte) error {
	return nil
}
