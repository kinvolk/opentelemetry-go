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

package trace

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/label"
)

type testSpanProcessor struct {
	name          string
	spansStarted  []otel.Span
	spansEnded    []otel.Span
	shutdownCount int
}

func (t *testSpanProcessor) OnStart(s otel.Span, pc otel.SpanContext) {
	kv := label.KeyValue{
		Key:   "OnStart",
		Value: label.StringValue(t.name),
	}
	s.SetAttributes(kv)
	t.spansStarted = append(t.spansStarted, s)
}

func (t *testSpanProcessor) OnEnd(s otel.Span) {
	kv := label.KeyValue{
		Key:   "OnEnd",
		Value: label.StringValue(t.name),
	}
	s.SetAttributes(kv)
	t.spansEnded = append(t.spansEnded, s)
}

func (t *testSpanProcessor) Shutdown(_ context.Context) error {
	t.shutdownCount++
	return nil
}

func (t *testSpanProcessor) ForceFlush() {
}

func TestRegisterSpanProcessort(t *testing.T) {
	name := "Register span processor before span starts"
	tp := basicTracerProvider(t)
	spNames := []string{"sp1", "sp2", "sp3"}
	sps := NewNamedTestSpanProcessors(spNames)

	for _, sp := range sps {
		tp.RegisterSpanProcessor(sp)
	}

	tr := tp.Tracer("SpanProcessor")
	_, s := tr.Start(context.Background(), "OnStart")
	s.End()
	wantCount := 1

	for _, sp := range sps {
		gotCount := len(sp.spansStarted)
		if gotCount != wantCount {
			t.Errorf("%s: started count: got %d, want %d\n", name, gotCount, wantCount)
		}
		gotCount = len(sp.spansEnded)
		if gotCount != wantCount {
			t.Errorf("%s: ended count: got %d, want %d\n", name, gotCount, wantCount)
		}

		c := 0
		sd := sp.spansStarted[0].(*span).makeSpanData()
		for _, kv := range sd.Attributes {
			if kv.Key != "OnStart" {
				continue
			}
			gotValue := kv.Value.AsString()
			if gotValue != spNames[c] {
				t.Errorf("%s: ordered attributes: got %s, want %s\n", name, gotValue, spNames[c])
			}
			c++
		}
		if c != len(spNames) {
			t.Errorf("%s: expected attributes(OnStart): got %d, want %d\n", name, c, len(spNames))
		}
	}
}

func TestUnregisterSpanProcessor(t *testing.T) {
	name := "Start span after unregistering span processor"
	tp := basicTracerProvider(t)
	spNames := []string{"sp1", "sp2", "sp3"}
	sps := NewNamedTestSpanProcessors(spNames)

	for _, sp := range sps {
		tp.RegisterSpanProcessor(sp)
	}

	tr := tp.Tracer("SpanProcessor")
	_, s := tr.Start(context.Background(), "OnStart")
	s.End()
	for _, sp := range sps {
		tp.UnregisterSpanProcessor(sp)
	}

	// start another span after unregistering span processor.
	_, s = tr.Start(context.Background(), "Start span after unregister")
	s.End()

	for _, sp := range sps {
		wantCount := 1
		gotCount := len(sp.spansStarted)
		if gotCount != wantCount {
			t.Errorf("%s: started count: got %d, want %d\n", name, gotCount, wantCount)
		}

		gotCount = len(sp.spansEnded)
		if gotCount != wantCount {
			t.Errorf("%s: ended count: got %d, want %d\n", name, gotCount, wantCount)
		}

		c := 0
		sd := sp.spansStarted[0].(*span).makeSpanData()
		for _, kv := range sd.Attributes {
			if kv.Key != "OnEnd" {
				continue
			}
			gotValue := kv.Value.AsString()
			if gotValue != spNames[c] {
				t.Errorf("%s: ordered attributes: got %s, want %s\n", name, gotValue, spNames[c])
			}
			c++
		}
		if c != len(spNames) {
			t.Errorf("%s: expected attributes(OnEnd): got %d, want %d\n", name, c, len(spNames))
		}
	}
}

func TestUnregisterSpanProcessorWhileSpanIsActive(t *testing.T) {
	name := "Unregister span processor while span is active"
	tp := basicTracerProvider(t)
	sp := NewTestSpanProcessor("sp")
	tp.RegisterSpanProcessor(sp)

	tr := tp.Tracer("SpanProcessor")
	_, span := tr.Start(context.Background(), "OnStart")
	tp.UnregisterSpanProcessor(sp)

	span.End()

	wantCount := 1
	gotCount := len(sp.spansStarted)
	if gotCount != wantCount {
		t.Errorf("%s: started count: got %d, want %d\n", name, gotCount, wantCount)
	}

	wantCount = 0
	gotCount = len(sp.spansEnded)
	if gotCount != wantCount {
		t.Errorf("%s: ended count: got %d, want %d\n", name, gotCount, wantCount)
	}
}

func TestSpanProcessorShutdown(t *testing.T) {
	name := "Increment shutdown counter of a span processor"
	tp := basicTracerProvider(t)
	sp := NewTestSpanProcessor("sp")
	if sp == nil {
		t.Fatalf("Error creating new instance of TestSpanProcessor\n")
	}
	tp.RegisterSpanProcessor(sp)

	wantCount := 1
	err := sp.Shutdown(context.Background())
	if err != nil {
		t.Error("Error shutting the testSpanProcessor down\n")
	}

	gotCount := sp.shutdownCount
	if wantCount != gotCount {
		t.Errorf("%s: wrong counter: got %d, want %d\n", name, gotCount, wantCount)
	}
}

func TestMultipleUnregisterSpanProcessorCalls(t *testing.T) {
	name := "Increment shutdown counter after first UnregisterSpanProcessor call"
	tp := basicTracerProvider(t)
	sp := NewTestSpanProcessor("sp")
	if sp == nil {
		t.Fatalf("Error creating new instance of TestSpanProcessor\n")
	}

	wantCount := 1

	tp.RegisterSpanProcessor(sp)
	tp.UnregisterSpanProcessor(sp)

	gotCount := sp.shutdownCount
	if wantCount != gotCount {
		t.Errorf("%s: wrong counter: got %d, want %d\n", name, gotCount, wantCount)
	}

	// Multiple UnregisterSpanProcessor should not trigger multiple Shutdown calls.
	tp.UnregisterSpanProcessor(sp)

	gotCount = sp.shutdownCount
	if wantCount != gotCount {
		t.Errorf("%s: wrong counter: got %d, want %d\n", name, gotCount, wantCount)
	}
}

func NewTestSpanProcessor(name string) *testSpanProcessor {
	return &testSpanProcessor{name: name}
}

func NewNamedTestSpanProcessors(names []string) []*testSpanProcessor {
	tsp := []*testSpanProcessor{}
	for _, n := range names {
		tsp = append(tsp, NewTestSpanProcessor(n))
	}
	return tsp
}
