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

type grpcSingleConnectionManager struct {
	connection *grpcConnection

	lock          sync.Mutex
	metricsClient colmetricpb.MetricsServiceClient
	tracesClient  coltracepb.TraceServiceClient
}

func (m *grpcSingleConnectionManager) getMetricsClientProxy() grpcMetricsClientProxy {
	return grpcMetricsClientProxy{
		grpcClientProxyBase: grpcClientProxyBase{
			clientLock: &m.lock,
			connection: m.connection,
		},
		client: m.metricsClient,
	}
}

func (m *grpcSingleConnectionManager) getTracesClientProxy() grpcTracesClientProxy {
	return grpcTracesClientProxy{
		grpcClientProxyBase: grpcClientProxyBase{
			clientLock: &m.lock,
			connection: m.connection,
		},
		client: m.tracesClient,
	}
}

func (m *grpcSingleConnectionManager) handleNewConnection(cc *grpc.ClientConn) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if cc != nil {
		m.metricsClient = colmetricpb.NewMetricsServiceClient(cc)
		m.tracesClient = coltracepb.NewTraceServiceClient(cc)
	} else {
		m.metricsClient = nil
		m.tracesClient = nil
	}
	return nil
}

func NewGRPCSingleConnectionManager(cfg GRPCConnectionConfig) ConnectionManager {
	m := &grpcSingleConnectionManager{}
	m.connection = newGRPCConnection(cfg, m.handleNewConnection)
	return m
}

func (m *grpcSingleConnectionManager) Start(ctx context.Context) error {
	m.connection.startConnection(ctx)
	return nil
}

func (m *grpcSingleConnectionManager) Stop(ctx context.Context) error {
	return m.connection.shutdown(ctx)
}

func (m *grpcSingleConnectionManager) ExportMetrics(ctx context.Context, cps metricsdk.CheckpointSet, selector metricsdk.ExportKindSelector) error {
	return uploadMetrics(ctx, cps, selector, m.getMetricsClientProxy())
}

func (m *grpcSingleConnectionManager) ExportTraces(ctx context.Context, sds []*tracesdk.SpanData) error {
	return uploadTraces(ctx, sds, m.getTracesClientProxy())
}
