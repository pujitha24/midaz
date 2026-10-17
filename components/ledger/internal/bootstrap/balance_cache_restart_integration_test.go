//go:build integration

// Copyright (c) 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by the Elastic License 2.0
// that can be found in the LICENSE file.

package bootstrap

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	authMiddleware "github.com/LerianStudio/lib-auth/v5/auth/middleware"
	libCommons "github.com/LerianStudio/lib-commons/v7/commons"
	libRedis "github.com/LerianStudio/lib-commons/v7/commons/redis"
	libObservability "github.com/LerianStudio/lib-observability/v4"
	libLog "github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/metrics"
	"github.com/danielgtaylor/huma/v2"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	httpin "github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/http/in"
	ledgerMiddleware "github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/http/in/middleware"
	transactionMongo "github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/mongodb/transaction"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/account"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/asset"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/balance"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/ledger"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/operation"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/operationroute"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/organization"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/portfolio"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/segment"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/transaction"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/transactionroute"
	transactionRedis "github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/redis/transaction"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/services/command"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/services/query"
	"github.com/LerianStudio/midaz/v4/pkg/mmodel"
	"github.com/LerianStudio/midaz/v4/pkg/utils"
	testutils "github.com/LerianStudio/midaz/v4/tests/utils"
	"github.com/LerianStudio/midaz/v4/tests/utils/chaos"
	mongotestutil "github.com/LerianStudio/midaz/v4/tests/utils/mongodb"
	postgrestestutil "github.com/LerianStudio/midaz/v4/tests/utils/postgres"
	redistestutil "github.com/LerianStudio/midaz/v4/tests/utils/redis"
)

// TestIntegrationBalanceCacheRestartFromOlderState posts transfers on the real create path, SIGKILLs and
// restarts a dedicated Valkey, posts again, and requires each balance, in PostgreSQL and in the cache, to
// equal its opening plus its operations' deltas, with one operation per balance version.
func TestIntegrationBalanceCacheRestartFromOlderState(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL, MongoDB and a dedicated Valkey")
	}

	t.Setenv("AUDIT_LOG_ENABLED", "false")

	t.Run("rdb-snapshot", func(t *testing.T) {
		// No append-only file: a restart loads the last snapshot, taken here explicitly.
		h := newBalanceCacheRestartHarness(t, "--appendonly", "no", "--save", "")
		h.transfer(t, 10)
		h.transfer(t, 20)
		h.waitSynced(t) // the snapshot then holds no sync claim lock
		require.NoError(t, h.redis.Save(context.Background()).Err())
		t.Log("snapshot taken (SAVE)")

		for _, amount := range []int64{30, 40, 50} {
			h.transfer(t, amount)
		}

		h.waitRowVersion(t, 5)
		h.restartValkey(t)

		for _, amount := range []int64{60, 70, 80, 90} {
			h.transfer(t, amount)
			h.waitSynced(t)
			h.logReconciler(t)
		}

		h.verifyTrail(t)
	})

	t.Run("aof-everysec-sigkill", func(t *testing.T) {
		h := newBalanceCacheRestartHarness(t, "--appendonly", "yes", "--appendfsync", "everysec")
		for _, amount := range []int64{10, 20, 30, 40} {
			h.transfer(t, amount)
		}

		// A claim lock caught in the AOF mid-flush comes back with its 10-minute TTL
		// and holds that balance's sync; the worker stays off across the kill.
		h.waitSynced(t)
		h.stopSync()
		h.transfer(t, 50)
		h.restartValkey(t)
		h.startSync(t)
		h.waitSynced(t)

		for _, amount := range []int64{60, 70, 80, 90} {
			h.transfer(t, amount)
			h.waitSynced(t)
			h.logReconciler(t)
		}

		h.verifyTrail(t)
	})
}

type balanceCacheRestartHarness struct {
	db        *sql.DB
	redis     *redis.Client
	redisConn *libRedis.Client
	redisRepo *transactionRedis.RedisConsumerRepository
	command   *command.UseCase
	query     *query.UseCase
	app       *fiber.App
	valkey    testcontainers.Container
	logger    *cleanupCapturingLogger
	reader    *sdkmetric.ManualReader
	factory   *metrics.MetricsFactory
	orgID     uuid.UUID
	ledgerID  uuid.UUID
	balances  [2]restartBalance // source, destination
	posted    []uuid.UUID
	stopSync  func()
}

type restartBalance struct {
	id      uuid.UUID
	alias   string
	opening decimal.Decimal
}

type restartState struct {
	available, onHold decimal.Decimal
	version           int64
}

func (s restartState) String() string {
	return fmt.Sprintf("%s/%s v%d", s.available, s.onHold, s.version)
}

func (s restartState) equal(other restartState) bool {
	return s.available.Equal(other.available) && s.onHold.Equal(other.onHold) && s.version == other.version
}

type restartOperation struct {
	order         int
	transactionID uuid.UUID
	before, after restartState
}

func newBalanceCacheRestartHarness(t *testing.T, valkeyArgs ...string) *balanceCacheRestartHarness {
	t.Helper()

	pgContainer := postgrestestutil.SetupContainer(t)
	mongoContainer := mongotestutil.SetupContainer(t)
	valkey, addr := startPinnedPortValkey(t, valkeyArgs...)

	connectionString := postgrestestutil.BuildConnectionString(pgContainer.Host, pgContainer.Port, pgContainer.Config)
	pgClient := postgrestestutil.CreatePostgresClient(t, connectionString, connectionString, pgContainer.Config.DBName, postgrestestutil.FindMigrationsPath(t, "transaction"))
	postgrestestutil.ApplyOnboardingSchema(t, pgContainer.DB)

	redisClient := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = redisClient.Close() })
	redisConn := redistestutil.CreateConnection(t, addr)
	redisRepo, err := transactionRedis.NewConsumerRedis(redisConn)
	require.NoError(t, err)

	transactionRepo := transaction.NewTransactionPostgreSQLRepository(pgClient, false)
	operationRepo := operation.NewOperationPostgreSQLRepository(pgClient)
	balanceRepo := balance.NewBalancePostgreSQLRepository(pgClient, false)
	ledgerRepo := ledger.NewLedgerPostgreSQLRepository(pgClient)
	accountRepo := account.NewAccountPostgreSQLRepository(pgClient)
	assetRepo := asset.NewAssetPostgreSQLRepository(pgClient)
	organizationRepo := organization.NewOrganizationPostgreSQLRepository(pgClient)
	portfolioRepo := portfolio.NewPortfolioPostgreSQLRepository(pgClient)
	segmentRepo := segment.NewSegmentPostgreSQLRepository(pgClient)
	operationRouteRepo := operationroute.NewOperationRoutePostgreSQLRepository(pgClient)
	transactionRouteRepo := transactionroute.NewTransactionRoutePostgreSQLRepository(pgClient, false)
	metadataRepo := transactionMongo.NewMetadataMongoDBRepository(mongotestutil.CreateConnection(t, mongoContainer.URI, mongoContainer.DBName))

	queryUseCase := &query.UseCase{
		OrganizationRepo: organizationRepo, LedgerRepo: ledgerRepo, AssetRepo: assetRepo,
		AccountRepo: accountRepo, PortfolioRepo: portfolioRepo, SegmentRepo: segmentRepo,
		TransactionRepo: transactionRepo, OperationRepo: operationRepo, BalanceRepo: balanceRepo,
		OperationRouteRepo: operationRouteRepo, TransactionRouteRepo: transactionRouteRepo,
		TransactionMetadataRepo: metadataRepo, TransactionRedisRepo: redisRepo,
		EngineWriteBehindRepo: redisRepo, EngineWriteBehindCodec: command.EngineWriteBehindEvidenceCodec{},
	}
	// Synchronous projection (RABBITMQ_TRANSACTION_ASYNC=false): completion runs inside the request.
	commandUseCase := &command.UseCase{
		OrganizationRepo: organizationRepo, LedgerRepo: ledgerRepo, AssetRepo: assetRepo,
		AccountRepo: accountRepo, PortfolioRepo: portfolioRepo, SegmentRepo: segmentRepo,
		TransactionRepo: transactionRepo, OperationRepo: operationRepo, BalanceRepo: balanceRepo,
		OperationRouteRepo: operationRouteRepo, TransactionRouteRepo: transactionRouteRepo,
		TransactionMetadataRepo: metadataRepo, TransactionRedisRepo: redisRepo,
		AtomicTransactionBatchIdempotencyRepo:  redisRepo,
		AtomicTransactionBatchProjectionReader: queryUseCase,
		TransactionReader:                      queryUseCase,
		TransactionEvidenceResolver:            rabbitEngineEvidenceResolver{repository: redisRepo},
		UUIDv7Generator:                        libCommons.GenerateUUIDv7,
		Clock:                                  time.Now,
	}

	logger := &cleanupCapturingLogger{Logger: libLog.NewNop()}
	reader, factory := newBalanceSyncReaderFactory(t)
	require.NoError(t, configureAppliedTransactionCompletion(NewRedisQueueConsumer(logger, commandUseCase, queryUseCase), commandUseCase, false, nil, nil))
	require.NoError(t, configureEngine(commandUseCase, integrationEngineClientProvider{client: redisClient}))

	orgID := postgrestestutil.CreateTestOrganization(t, pgContainer.DB)
	ledgerID := postgrestestutil.CreateTestLedger(t, pgContainer.DB, orgID)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]

	h := &balanceCacheRestartHarness{
		db: pgContainer.DB, redis: redisClient, redisConn: redisConn, redisRepo: redisRepo,
		command: commandUseCase, query: queryUseCase, valkey: valkey,
		logger: logger, reader: reader, factory: factory, orgID: orgID, ledgerID: ledgerID,
		balances: [2]restartBalance{
			seedRestartBalance(t, pgContainer.DB, orgID, ledgerID, "@source-"+suffix, 1000),
			seedRestartBalance(t, pgContainer.DB, orgID, ledgerID, "@destination-"+suffix, 0),
		},
	}
	h.app = h.newApp()

	config := redisClient.ConfigGet(context.Background(), "*").Val()
	t.Logf("valkey %s: appendonly=%s appendfsync=%s save=%q maxmemory-policy=%s", addr, config["appendonly"], config["appendfsync"], config["save"], config["maxmemory-policy"])

	h.startSync(t)

	return h
}

// startPinnedPortValkey starts a dedicated Valkey on a fixed host port, so the
// ledger's clients reach the same process again after it is killed and started.
func startPinnedPortValkey(t *testing.T, args ...string) (testcontainers.Container, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	hostPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())

	ctr, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8",
			ExposedPorts: []string{"6379/tcp"},
			Cmd:          append([]string{"valkey-server", "--maxmemory-policy", "noeviction"}, args...),
			WaitingFor: wait.ForAll(
				wait.ForLog("Ready to accept connections"),
				wait.ForListeningPort("6379/tcp"),
			).WithDeadline(60 * time.Second),
			HostConfigModifier: func(hc *container.HostConfig) {
				testutils.ApplyResourceLimits(hc, 128, 0.5)
				hc.PortBindings = mobynetwork.PortMap{mobynetwork.MustParsePort("6379/tcp"): {
					{HostIP: netip.MustParseAddr("0.0.0.0"), HostPort: hostPort},
				}}
			},
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(context.Background())
	require.NoError(t, err)

	return ctr, net.JoinHostPort(host, hostPort)
}

func seedRestartBalance(t *testing.T, db *sql.DB, orgID, ledgerID uuid.UUID, alias string, opening int64) restartBalance {
	t.Helper()

	accountID := postgrestestutil.CreateTestAccount(t, db, orgID, ledgerID, nil, alias, alias, "USD", nil)
	params := postgrestestutil.DefaultBalanceParams()
	params.Alias, params.Available = alias, decimal.NewFromInt(opening)

	return restartBalance{id: postgrestestutil.CreateTestBalance(t, db, orgID, ledgerID, accountID, params), alias: alias, opening: params.Available}
}

func (h *balanceCacheRestartHarness) newApp() *fiber.App {
	app := fiber.New()
	app.Use(ledgerMiddleware.ErrorEnvelope())
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(libObservability.ContextWithMetricFactory(libObservability.ContextWithLogger(c.Context(), h.logger), h.factory))

		return c.Next()
	})

	auth := &authMiddleware.AuthClient{Enabled: false}
	transactions := &httpin.TransactionHandler{Command: h.command, Query: h.query, TransactionBatchMaxSize: 50}
	balances := &httpin.BalanceHandler{Command: h.command, Query: h.query}
	mountHumaContracts(app, h.logger, humaContract{prefix: "/v1", mount: func(group fiber.Router, api huma.API) {
		httpin.RegisterTransactionHumaRoutesToApp(group, api, auth, transactions, nil)
		httpin.RegisterBalanceRoutesToApp(group, api, auth, balances, nil)
	}})

	return app
}

// startSync runs the production balance sync collector and flush on a
// cancellable context; runWorker itself only stops on a process signal.
func (h *balanceCacheRestartHarness) startSync(t *testing.T) {
	t.Helper()

	worker := NewBalanceSyncWorker(h.logger, h.command, BalanceSyncConfig{})
	collector := NewBalanceSyncCollector(worker.syncConfig.BatchSize, worker.syncConfig.FlushTimeout(), worker.syncConfig.PollInterval(), h.logger)
	ctx, cancel := context.WithCancel(libObservability.ContextWithMetricFactory(libObservability.ContextWithLogger(context.Background(), h.logger), h.factory))
	done := make(chan struct{})

	go func() {
		defer close(done)

		collector.Run(ctx, worker.flushBatch, worker.fetchSyncKeys, func(waitCtx context.Context) bool { return waitOrDone(waitCtx, worker.idleWait) })
	}()

	var once sync.Once

	h.stopSync = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(h.stopSync)
}

func (h *balanceCacheRestartHarness) transfer(t *testing.T, amount int64) {
	t.Helper()

	path := "/v1/organizations/" + h.orgID.String() + "/ledgers/" + h.ledgerID.String() + "/transactions/json"
	body := fmt.Sprintf(`{"description":"cache restart","send":{"asset":"USD","value":"%[1]d","source":{"from":[{"accountAlias":%[2]q,"amount":{"asset":"USD","value":"%[1]d"}}]},"distribute":{"to":[{"accountAlias":%[3]q,"amount":{"asset":"USD","value":"%[1]d"}}]}}}`,
		amount, h.balances[0].alias, h.balances[1].alias)
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Idempotency", "cache-restart-"+uuid.NewString())
	response := performEngineWriteBehindHTTPRequest(t, h.app, request)

	label := fmt.Sprintf("T%d", len(h.posted)+1)
	if !assert.Equalf(t, http.StatusCreated, response.status, "%s transfer %d: %s", label, amount, response.body) {
		return
	}

	var created struct {
		ID         uuid.UUID              `json:"id"`
		Operations []*operation.Operation `json:"operations"`
	}

	require.NoError(t, json.Unmarshal(response.body, &created))

	legs := make([]string, 0, len(created.Operations))
	for _, op := range created.Operations {
		legs = append(legs, fmt.Sprintf("%s %s %s -> %s", op.AccountAlias, op.Type, restartLeg(op.Balance), restartLeg(op.BalanceAfter)))
	}

	t.Logf("%s POST transfer %d -> HTTP %d: %s", label, amount, response.status, strings.Join(legs, "; "))
	h.posted = append(h.posted, created.ID)
	h.logReads(t, "after "+label)
}

func restartLeg(b operation.Balance) string {
	if b.Available == nil || b.OnHold == nil || b.Version == nil {
		return "<missing>"
	}

	return restartState{available: *b.Available, onHold: *b.OnHold, version: *b.Version}.String()
}

func (h *balanceCacheRestartHarness) logReads(t *testing.T, label string) {
	t.Helper()

	for _, b := range h.balances {
		path := "/v1/organizations/" + h.orgID.String() + "/ledgers/" + h.ledgerID.String() + "/balances/" + b.id.String()
		response := performEngineWriteBehindHTTPRequest(t, h.app, httptest.NewRequest(http.MethodGet, path, nil))

		var read mmodel.Balance

		_ = json.Unmarshal(response.body, &read)
		t.Logf("%s: GET %s balance -> HTTP %d, available %s onHold %s version %d", label, b.alias, response.status, read.Available, read.OnHold, read.Version)
		assert.Equalf(t, http.StatusOK, response.status, "balance read: %s", response.body)
	}
}

func (h *balanceCacheRestartHarness) cacheKey(b restartBalance) string {
	return utils.BalanceInternalKey(h.orgID, h.ledgerID, b.alias+"#default")
}

func (h *balanceCacheRestartHarness) row(b restartBalance) (restartState, error) {
	var s restartState

	err := h.db.QueryRow(`SELECT available, on_hold, version FROM balance WHERE id = $1`, b.id).Scan(&s.available, &s.onHold, &s.version)

	return s, err
}

func (h *balanceCacheRestartHarness) live(b restartBalance) (restartState, error) {
	cached, err := h.redisRepo.ListBalanceByKey(context.Background(), h.orgID, h.ledgerID, b.alias+"#default")
	if err != nil {
		return restartState{}, err
	}

	return restartState{available: cached.Available, onHold: cached.OnHold, version: cached.Version}, nil
}

func (h *balanceCacheRestartHarness) logStates(t *testing.T, label string, read func(restartBalance) (restartState, error)) {
	t.Helper()

	for _, b := range h.balances {
		state, err := read(b)
		t.Logf("%s: %s %s (err: %v)", label, b.alias, state, err)
	}
}

// waitSynced returns once the balance sync worker removed every balance of the
// pair from the schedule, which it does after its PostgreSQL write returns.
func (h *balanceCacheRestartHarness) waitSynced(t *testing.T) {
	t.Helper()

	require.Eventually(t, func() bool {
		for _, b := range h.balances {
			if !errors.Is(h.redis.ZScore(context.Background(), utils.BalanceSyncScheduleKey, h.cacheKey(b)).Err(), redis.Nil) {
				return false
			}
		}

		return true
	}, 30*time.Second, 50*time.Millisecond, "the balance sync worker must process every scheduled balance")
	h.logStates(t, "PostgreSQL row after the sync worker ran", h.row)
}

func (h *balanceCacheRestartHarness) waitRowVersion(t *testing.T, version int64) {
	t.Helper()

	require.Eventuallyf(t, func() bool {
		for _, b := range h.balances {
			if row, err := h.row(b); err != nil || row.version != version {
				return false
			}
		}

		return true
	}, 30*time.Second, 50*time.Millisecond, "the balance sync worker must persist version %d", version)
	h.logStates(t, "PostgreSQL row", h.row)
}

func (h *balanceCacheRestartHarness) restartValkey(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	keys := h.redis.DBSize(ctx).Val()
	h.logStates(t, "live cache before SIGKILL", h.live)

	orchestrator := chaos.NewOrchestrator(t)
	defer func() { _ = orchestrator.Close() }()

	id := h.valkey.GetContainerID()
	require.NoError(t, orchestrator.KillContainer(ctx, id, "SIGKILL"))
	require.Eventually(t, func() bool {
		running, err := orchestrator.IsContainerRunning(ctx, id)

		return err == nil && !running
	}, 30*time.Second, 20*time.Millisecond, "Valkey must stop after SIGKILL")
	require.NoError(t, orchestrator.StartContainer(ctx, id))
	require.NoError(t, orchestrator.WaitForContainerRunning(ctx, id, 30*time.Second))

	// Same address, so both ledger clients re-dial the restarted process on use.
	require.Eventually(t, func() bool {
		shared, err := h.redisConn.GetClient(ctx)

		return err == nil && shared.Ping(ctx).Err() == nil && h.redis.Ping(ctx).Err() == nil
	}, 30*time.Second, 100*time.Millisecond, "the ledger's Valkey clients must reconnect")

	var persistence []string

	for _, line := range strings.Split(h.redis.Info(ctx, "persistence").Val(), "\r\n") {
		if strings.HasPrefix(line, "aof_enabled:") || strings.HasPrefix(line, "rdb_last_load_keys_loaded:") {
			persistence = append(persistence, line)
		}
	}

	t.Logf("valkey SIGKILLed and started again: %d keys before the kill, %d after; %s", keys, h.redis.DBSize(ctx).Val(), strings.Join(persistence, " "))

	logs, err := h.valkey.Logs(ctx)
	require.NoError(t, err)

	raw, err := io.ReadAll(logs)
	require.NoError(t, err)
	require.NoError(t, logs.Close())

	for _, line := range strings.Split(string(raw), "\n") {
		if lower := strings.ToLower(line); strings.Contains(lower, "load") || strings.Contains(lower, "aof") || strings.Contains(lower, "truncat") {
			t.Logf("valkey log: %s", strings.TrimSpace(line))
		}
	}

	h.logStates(t, "live cache after restart", h.live)
	h.logReads(t, "after restart")
}

func (h *balanceCacheRestartHarness) logReconciler(t *testing.T) {
	t.Helper()

	pairs, _, err := h.query.ReconcileBalancesPage(context.Background(), uuid.Nil, 10)
	require.NoError(t, err)

	now := time.Now()

	for _, pair := range pairs {
		alias := pair.BalanceID.String()

		for _, b := range h.balances {
			if b.id == pair.BalanceID {
				alias = b.alias
			}
		}

		head := "no trail"
		if pair.Trail != nil {
			head = fmt.Sprintf("trail head %s/%s v%d", pair.Trail.Available, pair.Trail.OnHold, pair.Trail.Version)
		}

		t.Logf("balance reconciler: %s row %s/%s v%d vs %s -> %s with no grace, %s with the default grace",
			alias, pair.Available, pair.OnHold, pair.Version, head,
			query.ClassifyBalanceTrailPair(pair, now, 0), query.ClassifyBalanceTrailPair(pair, now, defaultBalanceReconcilerGraceMs*time.Millisecond))
	}
}

// verifyTrail checks each balance against the operations PostgreSQL holds for it,
// in the order the transfers were confirmed.
func (h *balanceCacheRestartHarness) verifyTrail(t *testing.T) {
	t.Helper()

	order := make(map[uuid.UUID]int, len(h.posted))
	for i, id := range h.posted {
		order[id] = i + 1
	}

	for _, b := range h.balances {
		expected := restartState{available: b.opening, onHold: decimal.Zero}
		previous := expected
		versions := map[int64]int{}

		var breaks []string

		operations := h.trail(t, b, order)
		for _, op := range operations {
			t.Logf("operation trail %s: T%d %s -> %s", b.alias, op.order, op.before, op.after)

			switch {
			case op.before.version < previous.version:
				breaks = append(breaks, fmt.Sprintf("version regression: T%d starts at %s, the previous operation ended at %s", op.order, op.before, previous))
			case op.before.version > previous.version || op.after.version != op.before.version+1:
				breaks = append(breaks, fmt.Sprintf("version gap: T%d moves %s -> %s after %s", op.order, op.before, op.after, previous))
			case !op.before.equal(previous):
				breaks = append(breaks, fmt.Sprintf("amount break: T%d starts at %s, the previous operation ended at %s", op.order, op.before, previous))
			}

			expected.available = expected.available.Add(op.after.available.Sub(op.before.available))
			expected.onHold = expected.onHold.Add(op.after.onHold.Sub(op.before.onHold))
			expected.version = max(expected.version, op.after.version)
			versions[op.after.version]++
			previous = op.after
		}

		var duplicated []int64

		for version, count := range versions {
			if count > 1 {
				duplicated = append(duplicated, version)
			}
		}

		slices.Sort(duplicated)

		row, err := h.row(b)
		require.NoError(t, err)

		live, err := h.live(b)
		require.NoError(t, err)

		t.Logf("%s: opening %s + deltas of %d operations = %s | PostgreSQL row %s | live cache %s | versions recorded twice %v",
			b.alias, b.opening, len(operations), expected, row, live, duplicated)
		assert.Truef(t, row.equal(expected), "%s: PostgreSQL balance %s differs from opening + operation deltas %s", b.alias, row, expected)
		assert.Truef(t, live.equal(expected), "%s: live cache balance %s differs from opening + operation deltas %s", b.alias, live, expected)
		assert.Emptyf(t, duplicated, "%s: balance versions recorded by more than one operation", b.alias)
		assert.Emptyf(t, breaks, "%s: operations that do not start where the previous one ended", b.alias)
	}

	h.logReconciler(t)
	h.logSignals(t)
}

func (h *balanceCacheRestartHarness) trail(t *testing.T, b restartBalance, order map[uuid.UUID]int) []restartOperation {
	t.Helper()

	rows, err := h.db.Query(`SELECT transaction_id, available_balance, on_hold_balance, balance_version_before,
		available_balance_after, on_hold_balance_after, balance_version_after
		FROM operation WHERE balance_id = $1 AND deleted_at IS NULL AND balance_affected`, b.id)
	require.NoError(t, err)

	defer rows.Close()

	var operations []restartOperation

	for rows.Next() {
		var op restartOperation
		require.NoError(t, rows.Scan(&op.transactionID, &op.before.available, &op.before.onHold, &op.before.version,
			&op.after.available, &op.after.onHold, &op.after.version))
		op.order = order[op.transactionID]
		operations = append(operations, op)
	}

	require.NoError(t, rows.Err())
	slices.SortFunc(operations, func(x, y restartOperation) int { return cmp.Compare(x.order, y.order) })

	return operations
}

// logSignals prints what an operator would have seen: every warning and error
// the run logged, the sync writes skipped by the version guard, and the metrics.
func (h *balanceCacheRestartHarness) logSignals(t *testing.T) {
	t.Helper()

	counts := map[string]int{}
	samples := map[string]map[string]any{}
	skipped := 0

	var keys []string

	h.logger.mu.Lock()
	t.Logf("captured %d log entries at every level; warnings and errors follow", len(h.logger.entries))

	for _, entry := range h.logger.entries {
		if entry.msg == "UpdateMany completed" && fmt.Sprint(entry.fields["updated"]) != fmt.Sprint(entry.fields["total"]) {
			skipped++
		}

		if entry.level > libLog.LevelWarn {
			continue
		}

		key := fmt.Sprintf("level %d: %s", entry.level, entry.msg)
		if counts[key] == 0 {
			keys = append(keys, key)
			samples[key] = entry.fields
		}

		counts[key]++
	}
	h.logger.mu.Unlock()

	for _, key := range keys {
		t.Logf("logged %s (x%d), first fields %v", key, counts[key], samples[key])
	}

	t.Logf("balance sync writes that updated fewer rows than they carried (Debug only): %d", skipped)

	var collected metricdata.ResourceMetrics

	require.NoError(t, h.reader.Collect(context.Background(), &collected))

	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				total := int64(0)
				for _, point := range sum.DataPoints {
					total += point.Value
				}

				t.Logf("metric %s = %d", m.Name, total)

				continue
			}

			t.Logf("metric %s (%T)", m.Name, m.Data)
		}
	}
}
