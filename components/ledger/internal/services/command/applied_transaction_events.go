// Copyright (c) 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by the Elastic License 2.0
// that can be found in the LICENSE file.

package command

import (
	"context"
	"sync"
	"time"

	"github.com/LerianStudio/midaz/v4/components/ledger/internal/adapters/postgres/transaction"
)

// AppliedTransactionEventPublisher dispatches the existing best-effort transaction,
// overdraft, and balance-change emitters after durable finalization.
type AppliedTransactionEventPublisher interface {
	PublishAppliedTransactionEvents(ctx context.Context, tran *transaction.Transaction, phase string)
}

var _ AppliedTransactionEventPublisher = (*UseCase)(nil)

// PublishAppliedTransactionEvents delegates to the same best-effort event dispatch
// used by the legacy transaction persistence path.
func (uc *UseCase) PublishAppliedTransactionEvents(ctx context.Context, tran *transaction.Transaction, phase string) {
	uc.dispatchTransactionEvents(ctx, tran, phase)
}

func (uc *UseCase) dispatchTransactionEvents(ctx context.Context, tran *transaction.Transaction, phase string) {
	// A noop completion persisted no operation: the completion that wrote them
	// already published, so a replay must stay silent on every channel.
	if phase == TransactionLifecyclePhaseNoop {
		return
	}

	// Send events asynchronously with context that preserves trace but survives parent cancellation.
	// Each emitter gets its own timeout budget so a slow earlier emitter cannot starve later ones.
	finish := uc.TrackEventDispatch()

	go func() {
		defer finish()

		base := context.WithoutCancel(ctx)

		runWithTimeout := func(fn func(context.Context)) {
			emitCtx, cancel := context.WithTimeout(base, asyncOperationTimeout)
			defer cancel()

			fn(emitCtx)
		}

		var wg sync.WaitGroup

		wg.Add(3)

		go func() {
			defer wg.Done()

			runWithTimeout(func(c context.Context) { uc.SendTransactionEvents(c, tran, phase) })
		}()
		go func() { defer wg.Done(); runWithTimeout(func(c context.Context) { uc.SendOverdraftEvents(c, tran) }) }()
		go func() {
			defer wg.Done()

			runWithTimeout(func(c context.Context) { uc.SendBalanceChangedEvents(c, tran) })
		}()

		wg.Wait()
	}()
}

// TrackEventDispatch counts work that may still emit streaming events until
// the returned func runs. Call it before the work starts, never inside it.
func (uc *UseCase) TrackEventDispatch() (finish func()) {
	uc.eventDispatches.Add(1)

	return func() { uc.eventDispatches.Add(-1) }
}

// WaitEventDispatches blocks until no tracked work is running or ctx ends, and
// returns how many were still running. It polls a counter instead of a
// WaitGroup because work may start while it waits.
func (uc *UseCase) WaitEventDispatches(ctx context.Context) int64 {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		running := uc.eventDispatches.Load()
		if running == 0 {
			return 0
		}

		select {
		case <-ctx.Done():
			return running
		case <-ticker.C:
		}
	}
}
