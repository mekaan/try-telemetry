package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mekaan/try-telemetry/internal/ingest"
	"github.com/mekaan/try-telemetry/internal/schema"
	"github.com/mekaan/try-telemetry/internal/store"
)

type recorder struct{ got []store.Event }

func (r *recorder) Apply(_ context.Context, e store.Event) (store.Outcome, error) {
	r.got = append(r.got, e)
	return store.Outcome{StateUpdated: true}, nil
}

var now = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func processor(t *testing.T) (*ingest.Processor, *recorder) {
	t.Helper()
	reg, err := schema.Load("../../schemas")
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	return &ingest.Processor{Schemas: reg, Store: rec, Now: func() time.Time { return now }}, rec
}

func ptr(i int) *int { return &i }

func event() ingest.Envelope {
	return ingest.Envelope{
		ChargerID: "DK-1", ConnectorID: ptr(1), EventType: "status_notification",
		OccurredAt: now.Add(-time.Minute),
		Payload:    json.RawMessage(`{"status":"Available","error_code":"NoError"}`),
	}
}

func TestProcessRejects(t *testing.T) {
	tests := map[string]func(*ingest.Envelope){
		"missing charger":   func(e *ingest.Envelope) { e.ChargerID = "" },
		"missing connector": func(e *ingest.Envelope) { e.ConnectorID = nil },
		"huge connector":    func(e *ingest.Envelope) { e.ConnectorID = ptr(3_000_000_000) },
		"NUL in charger_id": func(e *ingest.Envelope) { e.ChargerID = "X\x00" },
		"huge charger_id":   func(e *ingest.Envelope) { e.ChargerID = strings.Repeat("A", 200_000) },
		"odd event_id":      func(e *ingest.Envelope) { e.EventID = "id with spaces" },
		"NUL in payload": func(e *ingest.Envelope) {
			e.Payload = json.RawMessage(`{"status":"Available","error_code":"NoError","info":"a\u0000b"}`)
		},
		"negative connector":  func(e *ingest.Envelope) { e.ConnectorID = ptr(-1) },
		"unknown type":        func(e *ingest.Envelope) { e.EventType = "made_up" },
		"unknown version":     func(e *ingest.Envelope) { e.SchemaVersion = 7 },
		"clock in the future": func(e *ingest.Envelope) { e.OccurredAt = now.Add(time.Hour) },
		"older than window":   func(e *ingest.Envelope) { e.OccurredAt = time.Unix(0, 0) },
		"bad enum":            func(e *ingest.Envelope) { e.Payload = json.RawMessage(`{"status":"Sleeping","error_code":"NoError"}`) },
		"missing field":       func(e *ingest.Envelope) { e.Payload = json.RawMessage(`{"status":"Available"}`) },
		"unexpected field": func(e *ingest.Envelope) {
			e.Payload = json.RawMessage(`{"status":"Available","error_code":"NoError","colour":"red"}`)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			p, rec := processor(t)
			e := event()
			mutate(&e)
			_, _, err := p.Process(context.Background(), e)
			if !ingest.IsInvalid(err) {
				t.Fatalf("err = %v, want InvalidError", err)
			}
			if len(rec.got) != 0 {
				t.Fatal("invalid event reached the store")
			}
		})
	}
}

func TestProcessDefaults(t *testing.T) {
	p, rec := processor(t)
	e := event()
	e.OccurredAt = e.OccurredAt.In(time.FixedZone("CEST", 2*3600)).Add(123 * time.Nanosecond)
	if _, _, err := p.Process(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	got := rec.got[0]
	if got.SchemaVersion != 1 {
		t.Errorf("schema_version = %d, want default 1", got.SchemaVersion)
	}
	if got.OccurredAt.Location() != time.UTC || got.OccurredAt.Nanosecond()%1000 != 0 {
		t.Errorf("occurred_at %v not normalised to UTC microseconds", got.OccurredAt)
	}
	if got.EventID == "" {
		t.Error("event_id not derived")
	}
}

// A producer retry without an event_id must produce the same id, or it would
// not dedupe. The same instant in another time zone, or the same payload with
// other key order and spacing, is the same reading.
func TestDeriveIDIsStable(t *testing.T) {
	a := store.Event{ChargerID: "DK-1", ConnectorID: 1, EventType: "status_notification", OccurredAt: now,
		Payload: json.RawMessage(`{"status":"Available","error_code":"NoError"}`)}
	b := a
	b.OccurredAt = a.OccurredAt.In(time.FixedZone("CEST", 2*3600))
	b.Payload = json.RawMessage(`{ "error_code": "NoError", "status": "Available" }`)
	if id(t, a) != id(t, b) {
		t.Error("same reading got different ids")
	}
	c := a
	c.ConnectorID = 2
	if id(t, a) == id(t, c) {
		t.Error("different connectors got the same id")
	}
	// Numbers are compared exactly, as Postgres does.
	big1, big2 := a, a
	big1.Payload = json.RawMessage(`{"n": 9007199254740992}`)
	big2.Payload = json.RawMessage(`{"n": 9007199254740993}`)
	if id(t, big1) == id(t, big2) {
		t.Error("integers above 2^53 got the same id")
	}
	// Two status changes in the same second are two events.
	d := a
	d.Payload = json.RawMessage(`{"status":"Charging","error_code":"NoError"}`)
	if id(t, a) == id(t, d) {
		t.Error("different payloads at the same instant got the same id")
	}
}

func id(t *testing.T, e store.Event) string {
	t.Helper()
	s, err := ingest.DeriveID(e)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPermanentErrors(t *testing.T) {
	p, _ := processor(t)
	e := event()
	e.EventType = "made_up"
	_, _, err := p.Process(context.Background(), e)
	if !ingest.IsPermanent(err) {
		t.Errorf("invalid event not permanent: %v", err)
	}
	if !ingest.IsPermanent(fmt.Errorf("apply: %w", store.ErrConflict)) {
		t.Error("conflict not permanent")
	}
	if ingest.IsPermanent(errors.New("connection refused")) {
		t.Error("database outage treated as permanent; it would be dead-lettered instead of retried")
	}
}
