// Copyright (c) 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by the Elastic License 2.0
// that can be found in the LICENSE file.

package bootstrap

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-observability/v4/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/midaz/v4/components/ledger/internal/services/command"
)

// A dispatch still emitting when the shutdown hook starts must reach the
// producer before it closes; closing first answers it with ErrEmitterClosed.
func TestDrainEventsThenCloseProducerClosesAfterTheRunningDispatch(t *testing.T) {
	uc := &command.UseCase{}
	finish := uc.TrackEventDispatch()

	var emitted, emittedBeforeClose atomic.Bool

	release := make(chan struct{})

	go func() {
		<-release
		emitted.Store(true)
		finish()
	}()

	hook := drainEventsThenCloseProducer(uc, func() error {
		emittedBeforeClose.Store(emitted.Load())
		return nil
	}, libLog.NewNop())

	returned := make(chan error, 1)

	go func() { returned <- hook(context.Background()) }()

	assert.Never(t, func() bool { return len(returned) > 0 }, 100*time.Millisecond, 10*time.Millisecond,
		"the hook must not close the producer while a dispatch runs")

	close(release)

	require.Eventually(t, func() bool { return len(returned) > 0 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, <-returned)
	assert.True(t, emittedBeforeClose.Load(), "the running dispatch emits before the producer closes")
}

// A dispatch that never ends must not hold the shutdown past the hook deadline:
// the producer still closes, so its flush runs inside the pod grace period.
func TestDrainEventsThenCloseProducerClosesAtTheDeadline(t *testing.T) {
	uc := &command.UseCase{}
	_ = uc.TrackEventDispatch()

	var closed atomic.Bool

	hook := drainEventsThenCloseProducer(uc, func() error {
		closed.Store(true)
		return nil
	}, libLog.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	returned := make(chan error, 1)

	go func() { returned <- hook(ctx) }()

	require.Eventually(t, func() bool { return len(returned) > 0 }, 2*time.Second, 10*time.Millisecond,
		"the hook returns once its deadline passes")
	require.NoError(t, <-returned)
	assert.True(t, closed.Load(), "the producer closes even with a dispatch still running")
}
