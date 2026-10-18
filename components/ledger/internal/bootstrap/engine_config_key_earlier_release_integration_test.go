//go:build integration

// Copyright (c) 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by the Elastic License 2.0
// that can be found in the LICENSE file.

package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/midaz/v4/pkg/constant"
	postgrestestutil "github.com/LerianStudio/midaz/v4/tests/utils/postgres"
)

// earlierReleaseBlock is the cache write of an account block served by
// v4.3.0-rc.4 (update_balance_blocked.lua): Blocked rewritten in place.
const earlierReleaseBlock = `
for i = 1, #KEYS do
	local cur = redis.call('GET', KEYS[i])
	if cur then
		local ok, balance = pcall(cjson.decode, cur)
		if ok and type(balance) == 'table' then
			balance.blocked = nil
			balance.Blocked = tonumber(ARGV[1])
			redis.call('SET', KEYS[i], cjson.encode(balance), 'EX', tonumber(ARGV[2]))
		end
	end
end
return 0`

// TestIntegrationEngineHotBalanceTakesAChangeAnEarlierReleaseServed caches a balance
// and its keys on a current pod, changes PostgreSQL and the blob's copy as an earlier
// release's PATCH does (no key, no version field), and the next transfer obeys it.
func TestIntegrationEngineHotBalanceTakesAChangeAnEarlierReleaseServed(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL, MongoDB, Valkey, and RabbitMQ")
	}

	t.Setenv("ALLOW_INSECURE_TLS", "true")
	t.Setenv("AUDIT_LOG_ENABLED", "false")
	// The harness wires no broker producer for the overdraft events of a draw.
	t.Setenv("RABBITMQ_OVERDRAFT_EVENTS_ENABLED", "false")

	infra := setupEngineWriteBehindHTTPIntegration(t)
	infra.command.TransactionWriteBehindAsync = false
	app := infra.newHTTPApp("")

	t.Run("account blocked", func(t *testing.T) {
		ctx := context.Background()
		seeded := infra.seedColdTransfer(t, "hot-blocked")
		infra.warmTransfer(t, app, seeded)

		_, err := infra.db.ExecContext(ctx, `UPDATE account SET blocked = true WHERE id = $1`, seeded.sourceAccount)
		require.NoError(t, err)
		require.NoError(t, infra.redis.Eval(ctx, earlierReleaseBlock, []string{seeded.sourceBlobKey}, 1, 3600).Err())

		created := infra.postCreate(t, app, "v1", seeded.aliases, "hot-blocked-"+uuid.NewString())
		require.Equalf(t, http.StatusUnprocessableEntity, created.status, "a blocked account must not transact: %s", created.body)
		assert.Equal(t, constant.ErrAccountBlocked.Error(), created.decoded["code"])
	})

	t.Run("overdraft disabled", func(t *testing.T) {
		ctx := context.Background()
		seeded := infra.seedColdTransfer(t, "hot-overdraft")

		// 150 available with an overdraft: the first transfer of 100 draws nothing,
		// the second needs a draw of 50.
		_, err := infra.db.ExecContext(ctx, `UPDATE balance SET available = 150, settings = $2 WHERE id = $1`, seeded.sourceBalance,
			`{"balanceScope":"transactional","allowOverdraft":true,"overdraftLimitEnabled":false}`)
		require.NoError(t, err)

		params := postgrestestutil.DefaultBalanceParams()
		params.Alias, params.Key, params.Available = seeded.aliases.source, constant.OverdraftBalanceKey, decimal.Zero
		companionID := postgrestestutil.CreateTestBalance(t, infra.db, infra.organization, infra.ledger, seeded.sourceAccount, params)
		_, err = infra.db.ExecContext(ctx, `UPDATE balance SET direction = 'debit', settings = '{"balanceScope":"internal"}' WHERE id = $1`, companionID)
		require.NoError(t, err)

		infra.warmTransfer(t, app, seeded)

		_, err = infra.db.ExecContext(ctx, `UPDATE balance SET settings = $2 WHERE id = $1`, seeded.sourceBalance,
			`{"balanceScope":"transactional","allowOverdraft":false,"overdraftLimitEnabled":false}`)
		require.NoError(t, err)
		infra.rewriteBlobFields(t, ctx, seeded.sourceBlobKey, map[string]string{"AllowOverdraft": "0", "allowOverdraft": "false"})

		created := infra.postCreate(t, app, "v1", seeded.aliases, "hot-overdraft-"+uuid.NewString())
		require.Equalf(t, http.StatusUnprocessableEntity, created.status, "a disabled overdraft must not be drawn: %s", created.body)
		assert.Equal(t, constant.ErrInsufficientFunds.Error(), created.decoded["code"])
	})
}

// warmTransfer runs the first transfer of a cold pair on a current pod: it caches
// the source balance and writes its configuration keys.
func (infra *engineWriteBehindHTTPIntegration) warmTransfer(t *testing.T, app *fiber.App, seeded coldTransfer) {
	t.Helper()

	created := infra.postCreate(t, app, "v1", seeded.aliases, "warm-"+uuid.NewString())
	require.Equalf(t, http.StatusCreated, created.status, "the first transfer must commit: %s", created.body)

	keys, err := infra.redisRepo.ReadConfigKeys(context.Background(), infra.organization, infra.ledger,
		[]string{seeded.sourceRef}, []uuid.UUID{seeded.sourceAccount})
	require.NoError(t, err)
	require.Contains(t, keys.Balances, seeded.sourceRef)
	require.Contains(t, keys.Accounts, seeded.sourceAccount)
	require.Equal(t, int64(1), infra.redis.Exists(context.Background(), seeded.sourceBlobKey).Val(), "the source balance is cached")
}

// rewriteBlobFields changes raw fields of a cached blob in place. An earlier release's
// settings PATCH rewrites all four overdraft settings; the load compares content only,
// so the one that changed is enough.
func (infra *engineWriteBehindHTTPIntegration) rewriteBlobFields(t *testing.T, ctx context.Context, key string, changes map[string]string) {
	t.Helper()

	raw, err := infra.redis.Get(ctx, key).Bytes()
	require.NoError(t, err)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))

	for name, value := range changes {
		fields[name] = json.RawMessage(value)
	}

	rewritten, err := json.Marshal(fields)
	require.NoError(t, err)
	require.NoError(t, infra.redis.Set(ctx, key, rewritten, time.Hour).Err())
}
