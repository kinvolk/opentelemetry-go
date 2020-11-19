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

// Package otlp contains an exporter for the OpenTelemetry protocol buffers.
//
// This package is currently in a pre-GA phase. Backwards incompatible changes
// may be introduced in subsequent minor version releases as we work to track
// the evolving OpenTelemetry specification and user feedback.
//
// To create a new exporter use either NewExporter or
// NewUnstartedExporter. But to create one, a connection manager is
// required. Currently only a GRPC connection manager is provided, and
// it can be created with NewGRPCSingleConnectionManager. The GRPC
// connection manager can be configured. Please see the examples to
// see how it's done.
package otlp // import "go.opentelemetry.io/otel/exporters/otlp"
