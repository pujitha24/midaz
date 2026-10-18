// Copyright (c) 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by the Elastic License 2.0
// that can be found in the LICENSE file.

package services

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	pgdbMocks "github.com/LerianStudio/midaz/v4/components/tracer/internal/adapters/postgres/db/mocks"
	commandMocks "github.com/LerianStudio/midaz/v4/components/tracer/internal/services/command/mocks"
	"github.com/LerianStudio/midaz/v4/components/tracer/internal/services/mocks"
	queryMocks "github.com/LerianStudio/midaz/v4/components/tracer/internal/services/query/mocks"
	"github.com/LerianStudio/midaz/v4/components/tracer/internal/testutil"
	"github.com/LerianStudio/midaz/v4/components/tracer/pkg/model"
)

func rulesOnlyRequest() *model.ValidationRequest {
	return &model.ValidationRequest{
		RequestID:            testutil.MustDeterministicUUID(7001),
		TransactionType:      model.TransactionTypeCard,
		Amount:               decimal.RequireFromString("100"),
		Asset:                "BRL",
		TransactionTimestamp: time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
		Account:              model.AccountContext{ID: testutil.MustDeterministicUUID(7002)},
		RulesOnly:            true,
	}
}

// validateRulesOnly runs a new rules-only request whose rules decide `decision`
// and fails the test if the limit transaction is opened or a limit is checked.
// It returns the audit request snapshot.
func validateRulesOnly(t *testing.T, decision model.Decision) map[string]any {
	t.Helper()
	testutil.SetupTestTracing(t)

	ctrl := gomock.NewController(t)
	txBeginner := pgdbMocks.NewMockTxBeginner(ctrl)
	ruleEval := mocks.NewMockRuleEvaluator(ctrl)
	limitCheck := mocks.NewMockLimitChecker(ctrl)
	commandRepo := commandMocks.NewMockTransactionValidationRepository(ctrl)
	queryRepo := queryMocks.NewMockTransactionValidationRepository(ctrl)
	auditWriter := mocks.NewMockAuditWriter(ctrl)

	ruleID := testutil.MustDeterministicUUID(7010)
	evalResult, err := model.NewEvaluationResult(decision, []uuid.UUID{ruleID}, []uuid.UUID{ruleID}, "rule verdict")
	require.NoError(t, err)

	var snapshot map[string]any

	queryRepo.EXPECT().FindByRequestID(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
	ruleEval.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(evalResult, nil).Times(1)
	txBeginner.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Times(0)
	limitCheck.EXPECT().CheckLimits(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	commandRepo.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	auditWriter.EXPECT().
		RecordValidationEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uuid.UUID, request map[string]any, _ model.EvaluationResult, _ model.ValidationResponseContext) error {
			snapshot = request
			return nil
		}).
		Times(1)

	service, err := NewValidationService(txBeginner, ruleEval, limitCheck, commandRepo, queryRepo, auditWriter, nil)
	require.NoError(t, err)

	result, err := service.Validate(context.Background(), rulesOnlyRequest())
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.IsDuplicate)
	assert.Equal(t, decision, result.Response.Decision)
	assert.NotNil(t, result.Response.LimitUsageDetails)
	assert.Empty(t, result.Response.LimitUsageDetails)

	return snapshot
}

func TestValidationService_Validate_RulesOnly_AllowCountsNoLimits(t *testing.T) {
	snapshot := validateRulesOnly(t, model.DecisionAllow)
	assert.Equal(t, true, snapshot["rulesOnly"], "the audit snapshot must mark a rules-only verdict")

	fullRequest := rulesOnlyRequest()
	fullRequest.RulesOnly = false

	assert.NotContains(t, buildRequestSnapshot(fullRequest), "rulesOnly", "full-validation snapshots must stay byte-identical")
}

func TestValidationService_Validate_RulesOnly_ReviewCountsNoLimits(t *testing.T) {
	validateRulesOnly(t, model.DecisionReview)
}
