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

type grpcClientProxyBase struct {
	clientLock *sync.Mutex
	connection *grpcConnection
}

func (p grpcClientProxyBase) check() error {
	if !p.connection.connected() {
		return errDisconnected
	}
	return nil
}

func (p grpcClientProxyBase) unifyContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return p.connection.contextWithStop(ctx)
}

type grpcMetricsClientProxy struct {
	grpcClientProxyBase
	client colmetricpb.MetricsServiceClient
}

func (p grpcMetricsClientProxy) uploadMetrics(ctx context.Context, protoMetrics []*metricpb.ResourceMetrics) error {
	ctx = p.connection.contextWithMetadata(ctx)
	err := func() error {
		p.clientLock.Lock()
		defer p.clientLock.Unlock()
		if p.client == nil {
			return errNoClient
		}
		_, err := p.client.Export(ctx, &colmetricpb.ExportMetricsServiceRequest{
			ResourceMetrics: protoMetrics,
		})
		return err
	}()
	if err != nil {
		p.connection.setStateDisconnected(err)
	}
	return err
}

type grpcTracesClientProxy struct {
	grpcClientProxyBase
	client coltracepb.TraceServiceClient
}

func (p grpcTracesClientProxy) uploadTraces(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
	ctx = p.connection.contextWithMetadata(ctx)
	err := func() error {
		p.clientLock.Lock()
		defer p.clientLock.Unlock()
		if p.client == nil {
			return errNoClient
		}
		_, err := p.client.Export(ctx, &coltracepb.ExportTraceServiceRequest{
			ResourceSpans: protoSpans,
		})
		return err
	}()
	if err != nil {
		p.connection.setStateDisconnected(err)
	}
	return err
}

func uploadMetrics(ctx context.Context, cps metricsdk.CheckpointSet, selector metricsdk.ExportKindSelector, proxy grpcMetricsClientProxy) error {
	if err := proxy.check(); err != nil {
		return err
	}
	ctx, cancel := proxy.unifyContext(ctx)
	defer cancel()

	rms, err := transform.CheckpointSet(ctx, selector, cps, 1)
	if err != nil {
		return err
	}
	if len(rms) == 0 {
		return nil
	}

	return proxy.uploadMetrics(ctx, rms)
}

func uploadTraces(ctx context.Context, sds []*tracesdk.SpanData, proxy grpcTracesClientProxy) error {
	if err := proxy.check(); err != nil {
		return err
	}
	ctx, cancel := proxy.unifyContext(ctx)
	defer cancel()

	protoSpans := transform.SpanData(sds)
	if len(protoSpans) == 0 {
		return nil
	}

	return proxy.uploadTraces(ctx, protoSpans)
}
