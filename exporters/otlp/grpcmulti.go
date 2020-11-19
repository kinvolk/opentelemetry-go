// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp // import "go.opentelemetry.io/otel/exporters/otlp"

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	colmetricpb "go.opentelemetry.io/otel/exporters/otlp/internal/opentelemetry-proto-gen/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/otel/exporters/otlp/internal/opentelemetry-proto-gen/collector/trace/v1"
	metricpb "go.opentelemetry.io/otel/exporters/otlp/internal/opentelemetry-proto-gen/metrics/v1"
	tracepb "go.opentelemetry.io/otel/exporters/otlp/internal/opentelemetry-proto-gen/trace/v1"
	"go.opentelemetry.io/otel/exporters/otlp/internal/transform"
	metricsdk "go.opentelemetry.io/otel/sdk/export/metric"
	tracesdk "go.opentelemetry.io/otel/sdk/export/trace"
)

type grpcMultiConnectionManager struct {
	metricsConnection *grpcConnection
	tracesConnection  *grpcConnection

	metricsLock   sync.Mutex
	metricsClient colmetricpb.MetricsServiceClient

	tracesLock   sync.Mutex
	tracesClient coltracepb.TraceServiceClient
}

func (m *grpcMultiConnectionManager) getMetricsClientProxy() grpcMetricsClientProxy {
	return grpcMetricsClientProxy{
		grpcClientProxyBase: grpcClientProxyBase{
			clientLock: &m.metricsLock,
			connection: m.metricsConnection,
		},
		client: m.metricsClient,
	}
}

func (m *grpcMultiConnectionManager) getTracesClientProxy() grpcTracesClientProxy {
	return grpcTracesClientProxy{
		grpcClientProxyBase: grpcClientProxyBase{
			clientLock: &m.tracesLock,
			connection: m.tracesConnection,
		},
		client: m.tracesClient,
	}
}

func (m *grpcMultiConnectionManager) handleNewMetricsConnection(cc *grpc.ClientConn) error {
	m.metricsLock.Lock()
	defer m.metricsLock.Unlock()
	if cc != nil {
		m.metricsClient = colmetricpb.NewMetricsServiceClient(cc)
	} else {
		m.metricsClient = nil
	}
	return nil
}

func (m *grpcMultiConnectionManager) handleNewTracesConnection(cc *grpc.ClientConn) error {
	m.tracesLock.Lock()
	defer m.tracesLock.Unlock()
	if cc != nil {
		m.tracesClient = coltracepb.NewTraceServiceClient(cc)
	} else {
		m.tracesClient = nil
	}
	return nil
}

type GRPCMultiConnectionConfig struct {
	Traces  GRPCConnectionConfig
	Metrics GRPCConnectionConfig
}

func NewGRPCMultiConnectionManager(cfg GRPCMultiConnectionConfig) ConnectionManager {
	m := &grpcMultiConnectionManager{}
	m.metricsConnection = newGRPCConnection(cfg.Metrics, m.handleNewMetricsConnection)
	m.tracesConnection = newGRPCConnection(cfg.Traces, m.handleNewTracesConnection)
	return m
}

func (m *grpcMultiConnectionManager) Start(ctx context.Context) error {
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		m.metricsConnection.startConnection(ctx)
	}()
	go func() {
		defer wg.Done()
		m.tracesConnection.startConnection(ctx)
	}()
	wg.Wait()
	return nil
}

func (m *grpcMultiConnectionManager) Stop(ctx context.Context) error {
	var (
		metricErr error
		tracesErr error
	)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		metricErr = m.metricsConnection.shutdown(ctx)
	}()
	go func() {
		defer wg.Done()
		tracesErr = m.tracesConnection.shutdown(ctx)
	}()
	wg.Wait()
	if metricErr != nil {
		return metricErr
	}
	if tracesErr != nil {
		return tracesErr
	}
	return nil
}

func (m *grpcMultiConnectionManager) ExportMetrics(ctx context.Context, cps metricsdk.CheckpointSet, selector metricsdk.ExportKindSelector) error {
	return uploadMetrics(ctx, cps, selector, m.getMetricsClientProxy())
}

func (m *grpcMultiConnectionManager) ExportTraces(ctx context.Context, sds []*tracesdk.SpanData) error {
	return uploadTraces(ctx, sds, m.getTracesClientProxy())
}
