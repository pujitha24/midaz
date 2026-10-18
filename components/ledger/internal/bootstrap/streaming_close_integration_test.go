//go:build integration

// Copyright (c) 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by the Elastic License 2.0
// that can be found in the LICENSE file.

package bootstrap

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-observability/v4/log"
	libStreaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	pkgStreaming "github.com/LerianStudio/midaz/v4/pkg/streaming"
)

// A broker that stops answering must not hold the producer close past its
// bound: the lib's 30s fallback, after the HTTP drain and the event wait, would
// overrun the pod grace period and get the flush killed.
func TestIntegration_BuildStreamingEmitter_CloseIsBoundedWhenTheBrokerHangs(t *testing.T) {
	ctx := context.Background()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	appTopic, err := libStreaming.AppTopic(streamingITSource)
	require.NoError(t, err)

	dlqTopic, err := libStreaming.AppDLQTopic(streamingITSource)
	require.NoError(t, err)

	createTopics(t, ctx, cluster.ListenAddrs(), []string{appTopic, dlqTopic})

	produceSent := make(chan struct{})

	var once sync.Once

	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		once.Do(func() { close(produceSent) })

		return nil, nil, true // never answered: the record stays in flight
	})

	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", strings.Join(cluster.ListenAddrs(), ","))
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", streamingITSource)

	cfg := &Config{StreamingEnabled: true, StreamingCloudEventsSource: streamingITSource}

	emitter, closeProducer, err := BuildStreamingEmitter(ctx, cfg, libLog.NewNop(), nil)
	require.NoError(t, err)

	request, err := streamingITExpectations()[0].emitReq(pkgStreaming.DefaultTenantID)
	require.NoError(t, err)

	go func() { _ = emitter.Emit(ctx, request) }()

	select {
	case <-produceSent:
	case <-time.After(10 * time.Second):
		t.Fatal("the record never reached the broker")
	}

	closed := make(chan error, 1)

	go func() { closed <- closeProducer() }()

	select {
	case err := <-closed:
		require.ErrorIs(t, err, context.DeadlineExceeded, "the flush gave up at its deadline, not on a delivery")
	case <-time.After(producerCloseTimeout + 2*time.Second):
		t.Fatal("the producer close outlived its bound")
	}
}
