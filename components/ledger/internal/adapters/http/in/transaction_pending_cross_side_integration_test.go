// Copyright (c) 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by the Elastic License 2.0
// that can be found in the LICENSE file.

//go:build integration

package in

import (
	"context"
	nethttp "net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/midaz/v4/internal/cachepolicy"
	cn "github.com/LerianStudio/midaz/v4/pkg/constant"
	"github.com/LerianStudio/midaz/v4/pkg/utils"
	postgrestestutil "github.com/LerianStudio/midaz/v4/tests/utils/postgres"
)

// TestIntegration_TransactionV2Commit_SameAccountOnBothSidesHold proves a stored hold that names
// @source on both sides commits once: @source pays 4 net and @target receives 4.
func TestIntegration_TransactionV2Commit_SameAccountOnBothSidesHold(t *testing.T) {
	// NOT parallel: process-global huma state (see transaction_handler_v2_integration_test.go).
	t.Setenv("ALLOW_INSECURE_TLS", "true")

	infra := setupTestInfra(t)
	t.Setenv("RABBITMQ_TRANSACTION_ASYNC", "false")

	ctx := context.Background()
	db := infra.pgContainer.DB

	sourceID, targetID := seedTransfer(t, db, infra.orgID, infra.ledgerID, "@source", "@target", 100)
	_, otherID := seedTransfer(t, db, infra.orgID, infra.ledgerID, "@unused", "@other", 0)

	v2App := buildHumaV2DirectApp(t, infra.handler)

	body := `{"description":"three account hold","asset":"USD","amount":"10","debits":[{"alias":"@source",` + v2ScopeJSON + `,"amount":"10"}],` +
		`"credits":[{"alias":"@target",` + v2ScopeJSON + `,"amount":"4"},{"alias":"@other",` + v2ScopeJSON + `,"amount":"6"}]}`

	holdResp := decodeTxResponse(t, postV2Create(t, v2App, "hold", infra.orgID, infra.ledgerID, body, ""), nethttp.StatusCreated)
	holdID := uuid.MustParse(holdResp["id"].(string))
	drainBalanceSync(t, ctx, infra.handler.Command, infra.redisRepo, infra.orgID, infra.ledgerID)

	// Rewrite the stored @other credit onto @source; the body keeps entry keys ("1#@other#default").
	var stored string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT body::text FROM transaction WHERE id = $1`, holdID).Scan(&stored))
	require.Equal(t, 1, strings.Count(stored, `#@other#`), "the stored hold must name @other once; body: %s", stored)

	_, err := db.ExecContext(ctx, `UPDATE transaction SET body = $2::jsonb WHERE id = $1`,
		holdID, strings.Replace(stored, `#@other#`, `#@source#`, 1))
	require.NoError(t, err)

	// Drop the cached views of the hold, as for one opened long ago, so the commit rebuilds
	// it from the rewritten primary row.
	redisClient := infra.redisContainer.Client
	indexKey := "engine:" + cachepolicy.HashTag + ":transaction-index:" + infra.orgID.String() + ":" + infra.ledgerID.String()

	removed, err := redisClient.HDel(ctx, indexKey, holdID.String()).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), removed, "the hold's engine index entry must exist before its removal")
	require.NoError(t, redisClient.Del(ctx, utils.WriteBehindTransactionKey(infra.orgID, infra.ledgerID, holdID.String())).Err())

	_ = decodeTxResponse(t, postTransaction(t, v2App, v2CommitURL(infra.orgID, infra.ledgerID, holdID), "", ""), nethttp.StatusCreated)
	drainBalanceSync(t, ctx, infra.handler.Command, infra.redisRepo, infra.orgID, infra.ledgerID)
	assert.Equal(t, cn.APPROVED, postgrestestutil.GetTransactionStatus(t, db, holdID), "the committed hold should be APPROVED")

	requireDecimalEqual(t, decimal.NewFromInt(96), postgrestestutil.GetBalanceAvailable(t, db, sourceID), "@source pays the 4 its hold sent away")
	requireDecimalEqual(t, decimal.Zero, postgrestestutil.GetBalanceOnHold(t, db, sourceID), "@source releases the whole hold")
	requireDecimalEqual(t, decimal.NewFromInt(4), postgrestestutil.GetBalanceAvailable(t, db, targetID), "@target receives its 4")
	requireDecimalEqual(t, decimal.Zero, postgrestestutil.GetBalanceAvailable(t, db, otherID), "@other is no longer in the hold")

	rows := fetchOperationRows(t, db, holdID)
	legs := make(map[string]string, len(rows))

	for _, op := range rows {
		legs[op.AccountAlias+" "+op.Type] = op.Amount.String()
	}

	require.Len(t, rows, 4, "the hold and its commit post each leg once")
	assert.Equal(t, map[string]string{
		"@source " + cn.ONHOLD: "10", "@source " + cn.DEBIT: "10", "@source " + cn.CREDIT: "6", "@target " + cn.CREDIT: "4",
	}, legs)
}
