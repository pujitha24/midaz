// Copyright (c) 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by the Elastic License 2.0
// that can be found in the LICENSE file.

package completion

import (
	"context"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	libObservability "github.com/LerianStudio/lib-observability/v4"
	libLog "github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/metrics"

	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/operation"
	"github.com/LerianStudio/midaz/v4/pkg/utils"
)

// WithMetricsFactory sets the factory for the forked balance version counter; nil emits none.
func (store *Store) WithMetricsFactory(factory *metrics.MetricsFactory) *Store {
	store.metrics = factory

	return store
}

// forkTwinColumn finds another live operation at the same balance version; replays reuse the id.
const forkTwinColumn = `(SELECT twin.id FROM operation twin WHERE operation.balance_version_after > 0 AND twin.organization_id = operation.organization_id AND twin.ledger_id = operation.ledger_id
AND twin.account_id = operation.account_id AND twin.balance_id = operation.balance_id AND twin.balance_version_after = operation.balance_version_after
AND twin.deleted_at IS NULL AND twin.balance_affected = true AND twin.id <> operation.id ORDER BY twin.id LIMIT 1)`

// reportForks logs and counts each committed fork; it never alters the outcome.
func (store *Store) reportForks(ctx context.Context, forks map[*operation.Operation]string) {
	if len(forks) == 0 {
		return
	}

	logger := libObservability.NewLoggerFromContext(ctx)
	tenantID := tmcore.GetTenantIDContext(ctx)

	var counter *metrics.CounterBuilder

	if store.metrics != nil {
		var err error
		if counter, err = store.metrics.Counter(utils.BalanceVersionForked); err != nil {
			logger.Log(ctx, libLog.LevelDebug, "Failed to build balance version fork counter", libLog.Err(err))
		}
	}

	for row, twin := range forks {
		logger.Log(ctx, libLog.LevelError, "Balance version recorded by two operations: the balance forked from its operations",
			libLog.String("organization_id", row.OrganizationID), libLog.String("ledger_id", row.LedgerID),
			libLog.String("tenant_id", tenantID), libLog.String("balance_id", row.BalanceID),
			libLog.String("transaction_id", row.TransactionID), libLog.String("operation_id", row.ID),
			libLog.String("twin_operation_id", twin),
			libLog.Any("balance_version_before", row.Balance.Version), libLog.Any("balance_version_after", row.BalanceAfter.Version))

		if counter == nil {
			continue
		}

		labels := map[string]string{"organization_id": row.OrganizationID, "ledger_id": row.LedgerID, "tenant_id": tenantID}
		if err := counter.WithLabels(labels).Add(ctx, 1); err != nil {
			logger.Log(ctx, libLog.LevelDebug, "Failed to emit balance version fork counter", libLog.Err(err))
		}
	}
}
