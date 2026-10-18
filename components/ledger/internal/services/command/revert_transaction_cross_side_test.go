// Copyright (c) 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by the Elastic License 2.0
// that can be found in the LICENSE file.

package command

import (
	"context"
	"testing"
	"time"

	libObservability "github.com/LerianStudio/lib-observability/v4"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/operation"
	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/transaction"
	"github.com/LerianStudio/midaz/v4/pkg"
	"github.com/LerianStudio/midaz/v4/pkg/constant"
	"github.com/LerianStudio/midaz/v4/pkg/mmodel"
	"github.com/LerianStudio/midaz/v4/pkg/mtransaction"
)

// crossSideOrigin is a settled transaction an older build accepted with one account on
// both sides: @payer is debited 100 and credited 40 back, @payee is credited 60. Its
// reversal carries @payer on both sides too.
func crossSideOrigin() *transaction.Transaction {
	origin := revertibleOrigin()
	hundred := decimal.NewFromInt(100)
	sixty := decimal.NewFromInt(60)
	forty := decimal.NewFromInt(40)

	origin.Operations = []*operation.Operation{
		{Type: constant.DEBIT, AccountAlias: "@payer", Amount: operation.Amount{Value: &hundred}, AssetCode: "BRL", BalanceKey: "default"},
		{Type: constant.CREDIT, AccountAlias: "@payee", Amount: operation.Amount{Value: &sixty}, AssetCode: "BRL", BalanceKey: "default"},
		{Type: constant.CREDIT, AccountAlias: "@payer", Amount: operation.Amount{Value: &forty}, AssetCode: "BRL", BalanceKey: "default"},
	}

	return origin
}

// crossSideSend is the same shape as crossSideOrigin offered as a new send.
func crossSideSend() mtransaction.Transaction {
	return mtransaction.Transaction{
		Send: mtransaction.Send{
			Asset: "BRL",
			Value: decimal.NewFromInt(100),
			Source: mtransaction.Source{From: []mtransaction.FromTo{{
				AccountAlias: "@payer",
				Amount:       &mtransaction.Amount{Asset: "BRL", Value: decimal.NewFromInt(100)},
				IsFrom:       true,
			}}},
			Distribute: mtransaction.Distribute{To: []mtransaction.FromTo{
				{AccountAlias: "@payee", Amount: &mtransaction.Amount{Asset: "BRL", Value: decimal.NewFromInt(60)}},
				{AccountAlias: "@payer", Amount: &mtransaction.Amount{Asset: "BRL", Value: decimal.NewFromInt(40)}},
			}},
		},
	}
}

// TestRevertTransaction_CrossSideOriginStaysReversible proves a settled transaction that
// carries one account on both sides can still be reverted on either contract: both
// validation passes accept the rebuilt reversal and the pipeline reaches the balance read.
func TestRevertTransaction_CrossSideOriginStaysReversible(t *testing.T) {
	for _, tc := range []struct {
		name   string
		invoke func(uc *UseCase, in RevertTransactionInput) error
	}{
		{name: "v1", invoke: func(uc *UseCase, in RevertTransactionInput) error {
			_, _, err := uc.RevertTransactionV1(context.Background(), in)

			return err
		}},
		{name: "v2", invoke: func(uc *UseCase, in RevertTransactionInput) error {
			_, _, err := uc.RevertTransactionV2(context.Background(), in)

			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &revertReader{origin: crossSideOrigin()}
			uc := newRevertUseCase(t, reader)

			err := tc.invoke(uc, revertInput())

			require.ErrorIs(t, err, errBalancesUnavailable, "the reversal must pass validation and reach the balance read")
			assert.Equal(t, 1, reader.getBalancesCalls)
		})
	}
}

// TestCreateTransaction_CrossSideSendIsAmbiguous is the counterpart: the same shape
// offered as a new send, direct or pending, is refused before the balance read.
func TestCreateTransaction_CrossSideSendIsAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		v1           bool
	}{{"v2 direct", constant.CREATED, false}, {"v2 pending", constant.PENDING, false}, {"v1 pending", constant.PENDING, true}} {
		t.Run(tc.name, func(t *testing.T) {
			uc, reader, redisRepo := newVersionUseCase(t, mmodel.LedgerSettings{})

			redisRepo.EXPECT().Del(gomock.Any(), gomock.Any()).Return(nil).Times(1)

			send := crossSideSend()
			send.Pending = tc.status == constant.PENDING

			var err error
			if tc.v1 {
				_, _, err = uc.CreateTransactionV1(context.Background(), CreateTransactionV1Input{
					OrganizationID: uuid.New(), LedgerID: uuid.New(), Transaction: send, TransactionStatus: tc.status, IdempotencyTTL: time.Minute,
				})
			} else {
				_, _, err = uc.CreateTransactionV2(context.Background(), CreateTransactionV2Input{
					OrganizationID: uuid.New(), LedgerID: uuid.New(), Transaction: send, TransactionStatus: tc.status, IdempotencyTTL: time.Minute,
				})
			}

			require.Error(t, err)

			var business pkg.UnprocessableOperationError

			require.ErrorAs(t, err, &business)
			assert.Equal(t, constant.ErrTransactionAmbiguous.Error(), business.Code)
			assert.Zero(t, reader.getBalancesCalls, "the refusal must precede the balance read")
		})
	}
}

// TestAtomicTransactionBatchItem_CrossSideOnlyForRevert proves the batch item preparation
// lifts the shared-account rule for a revert item alone: the same body as a direct or hold
// item is refused with the ambiguity code.
func TestAtomicTransactionBatchItem_CrossSideOnlyForRevert(t *testing.T) {
	// The reversal of crossSideOrigin: @payee and @payer are debited, @payer is credited.
	reversal := mtransaction.Transaction{
		Send: mtransaction.Send{
			Asset: "BRL",
			Value: decimal.NewFromInt(100),
			Source: mtransaction.Source{From: []mtransaction.FromTo{
				{AccountAlias: "@payee", Amount: &mtransaction.Amount{Asset: "BRL", Value: decimal.NewFromInt(60)}, IsFrom: true},
				{AccountAlias: "@payer", Amount: &mtransaction.Amount{Asset: "BRL", Value: decimal.NewFromInt(40)}, IsFrom: true},
			}},
			Distribute: mtransaction.Distribute{To: []mtransaction.FromTo{{
				AccountAlias: "@payer",
				Amount:       &mtransaction.Amount{Asset: "BRL", Value: decimal.NewFromInt(100)},
			}}},
		},
	}

	for _, tc := range []struct {
		name     string
		action   string
		status   string
		wantCode string
	}{
		{name: "revert item", action: constant.ActionRevert, status: constant.CREATED},
		{name: "direct item", action: constant.ActionDirect, status: constant.CREATED, wantCode: constant.ErrTransactionAmbiguous.Error()},
		{name: "hold item", action: constant.ActionHold, status: constant.PENDING, wantCode: constant.ErrTransactionAmbiguous.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, spans, _, _ := libObservability.NewTrackingFromContext(context.Background())
			ctx, span := spans.Start(context.Background(), "test.cross_side_batch_item")
			t.Cleanup(func() { span.End() })

			item := atomicTransactionBatchItemRun{
				organizationID: uuid.New(),
				ledgerID:       uuid.New(),
				input:          reversal,
				status:         tc.status,
				action:         tc.action,
			}

			err := (&UseCase{}).prepareAtomicTransactionBatchItem(ctx, span, logger, &item)

			if tc.wantCode == "" {
				require.NoError(t, err)
				require.NotNil(t, item.validate)
				assert.Len(t, item.validate.Sources, 2)
				assert.Len(t, item.validate.Destinations, 1)

				return
			}

			require.Error(t, err)

			var business pkg.UnprocessableOperationError

			require.ErrorAs(t, err, &business)
			assert.Equal(t, tc.wantCode, business.Code)
		})
	}
}
