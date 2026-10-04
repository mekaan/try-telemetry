package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mekaan/try-telemetry/internal/store"
)

// These tests run against a real Postgres, because the correctness lives in
// the SQL. `make test` starts one; without DATABASE_URL they are skipped.
func open(t *testing.T) *store.Store {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	s, err := store.Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

var t0 = time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)

func status(charger, id string, at time.Time, value string) store.Event {
	return store.Event{
		EventID: id, ChargerID: charger, ConnectorID: 1, EventType: "status_notification",
		SchemaVersion: 1, OccurredAt: at,
		Payload: json.RawMessage(fmt.Sprintf(`{"status":%q,"error_code":"NoError"}`, value)),
	}
}

func currentStatus(t *testing.T, s *store.Store, charger string) string {
	t.Helper()
	state, err := s.LatestState(context.Background(), charger)
	if err != nil {
		t.Fatal(err)
	}
	var p struct{ Status string }
	if err := json.Unmarshal(state[1]["status_notification"].Payload, &p); err != nil {
		t.Fatal(err)
	}
	return p.Status
}

func historyLen(t *testing.T, s *store.Store, charger string) int {
	t.Helper()
	h, err := s.History(context.Background(), charger, 500)
	if err != nil {
		t.Fatal(err)
	}
	return len(h)
}

func TestApply(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	tests := []struct {
		name        string
		events      []store.Event
		wantStatus  string
		wantHistory int
		wantLast    store.Outcome // outcome of the final event
	}{
		{
			name:        "first event sets state",
			events:      []store.Event{status("c", "e1", t0, "Charging")},
			wantStatus:  "Charging",
			wantHistory: 1,
			wantLast:    store.Outcome{StateUpdated: true},
		},
		{
			name:        "duplicate is a no-op",
			events:      []store.Event{status("c", "e1", t0, "Charging"), status("c", "e1", t0, "Charging")},
			wantStatus:  "Charging",
			wantHistory: 1,
			wantLast:    store.Outcome{Duplicate: true},
		},
		{
			name:        "late event goes to history but not state",
			events:      []store.Event{status("c", "e2", t0, "Charging"), status("c", "e1", t0.Add(-time.Minute), "Preparing")},
			wantStatus:  "Charging",
			wantHistory: 2,
			wantLast:    store.Outcome{},
		},
		{
			name:        "newer event replaces state",
			events:      []store.Event{status("c", "e1", t0, "Charging"), status("c", "e2", t0.Add(time.Minute), "Finishing")},
			wantStatus:  "Finishing",
			wantHistory: 2,
			wantLast:    store.Outcome{StateUpdated: true},
		},
		{
			name:        "same timestamp resolves by event_id in arrival order a,b",
			events:      []store.Event{status("c", "a", t0, "Charging"), status("c", "b", t0, "Faulted")},
			wantStatus:  "Faulted",
			wantHistory: 2,
			wantLast:    store.Outcome{StateUpdated: true},
		},
		{
			name:        "same timestamp resolves by event_id in arrival order b,a",
			events:      []store.Event{status("c", "b", t0, "Faulted"), status("c", "a", t0, "Charging")},
			wantStatus:  "Faulted",
			wantHistory: 2,
			wantLast:    store.Outcome{},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			charger := fmt.Sprintf("test-%d-%d", time.Now().UnixNano(), i)
			var last store.Outcome
			for _, e := range tt.events {
				e.ChargerID = charger
				var err error
				if last, err = s.Apply(ctx, e); err != nil {
					t.Fatal(err)
				}
			}
			if got := currentStatus(t, s, charger); got != tt.wantStatus {
				t.Errorf("status = %q, want %q", got, tt.wantStatus)
			}
			if got := historyLen(t, s, charger); got != tt.wantHistory {
				t.Errorf("history = %d, want %d", got, tt.wantHistory)
			}
			if last != tt.wantLast {
				t.Errorf("last outcome = %+v, want %+v", last, tt.wantLast)
			}
		})
	}
}

// A late meter reading must still update meter state even though a newer
// status change arrived first. This is why latest state is keyed per type.
func TestEventTypesAreIndependent(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	charger := fmt.Sprintf("test-types-%d", time.Now().UnixNano())

	st := status(charger, charger+"-s", t0.Add(time.Minute), "Finishing")
	mv := store.Event{
		EventID: charger + "-m", ChargerID: charger, ConnectorID: 1, EventType: "meter_values",
		SchemaVersion: 1, OccurredAt: t0, Payload: json.RawMessage(`{"energy_active_import_wh": 1200}`),
	}
	for _, e := range []store.Event{st, mv} {
		out, err := s.Apply(ctx, e)
		if err != nil {
			t.Fatal(err)
		}
		if !out.StateUpdated {
			t.Errorf("%s: state not updated", e.EventType)
		}
	}
	state, err := s.LatestState(ctx, charger)
	if err != nil {
		t.Fatal(err)
	}
	if len(state[1]) != 2 {
		t.Errorf("connector 1 has %d event types in state, want 2", len(state[1]))
	}
}

// Event ids only need to be unique per charger. Two gateways that both
// number their messages from 1 must not swallow each other's events.
func TestEventIDIsPerCharger(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	base := fmt.Sprintf("test-scope-%d", time.Now().UnixNano())
	for _, charger := range []string{base + "-a", base + "-b"} {
		out, err := s.Apply(ctx, status(charger, "msg-1", t0, "Charging"))
		if err != nil {
			t.Fatal(err)
		}
		if out.Duplicate {
			t.Errorf("%s: treated as duplicate of another charger's event", charger)
		}
	}
}

func TestReusedIDWithDifferentContentConflicts(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	charger := fmt.Sprintf("test-conflict-%d", time.Now().UnixNano())
	if _, err := s.Apply(ctx, status(charger, "e1", t0, "Charging")); err != nil {
		t.Fatal(err)
	}
	// Same content, different JSON spelling: still a duplicate.
	same := status(charger, "e1", t0, "Charging")
	same.Payload = json.RawMessage(`{ "error_code": "NoError", "status": "Charging" }`)
	if out, err := s.Apply(ctx, same); err != nil || !out.Duplicate {
		t.Fatalf("same event: out=%+v err=%v, want duplicate", out, err)
	}
	if _, err := s.Apply(ctx, status(charger, "e1", t0, "Faulted")); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}

// The repair path after a bad release: only what the release wrote is undone.
func TestRepairWindow(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	charger := fmt.Sprintf("test-repair-%d", time.Now().UnixNano())
	apply := func(e store.Event) {
		t.Helper()
		if _, err := s.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	meter := func(id string, at time.Time, wh int) store.Event {
		return store.Event{EventID: id, ChargerID: charger, ConnectorID: 1, EventType: "meter_values",
			SchemaVersion: 1, OccurredAt: at, Payload: json.RawMessage(fmt.Sprintf(`{"energy_active_import_wh": %d}`, wh))}
	}

	// Before the release: a good status and a good meter reading.
	apply(status(charger, "good-status", t0, "Charging"))
	apply(meter("good-meter", t0, 100))
	time.Sleep(20 * time.Millisecond)

	// The bad release writes a wrong status. The meter state is untouched.
	from := time.Now()
	apply(status(charger, "bad-status", t0.Add(time.Minute), "Faulted"))
	to := time.Now().Add(time.Second)

	deleted, rebuilt, err := s.RepairWindow(ctx, from, to)
	if err != nil {
		t.Fatal(err)
	}
	if deleted < 1 || rebuilt < 1 {
		t.Errorf("deleted=%d rebuilt=%d, want at least 1 each", deleted, rebuilt)
	}
	if got := currentStatus(t, s, charger); got != "Charging" {
		t.Errorf("status after repair = %q, want the pre-release Charging", got)
	}
	state, err := s.LatestState(ctx, charger)
	if err != nil {
		t.Fatal(err)
	}
	if state[1]["meter_values"].EventID != "good-meter" {
		t.Errorf("meter state = %+v, want it left alone", state[1]["meter_values"])
	}
	if got := historyLen(t, s, charger); got != 2 {
		t.Errorf("history = %d, want the bad event removed", got)
	}
}

// The last line of defence against events that block a partition: if the
// database refuses the data itself, the error must be recognisable as
// permanent, not retried forever.
func TestDataErrorsAreRecognised(t *testing.T) {
	s := open(t)
	e := status(fmt.Sprintf("test-nul-%d", time.Now().UnixNano()), "e1", t0, "Charging")
	e.Payload = json.RawMessage(`{"status":"Charging","error_code":"NoError","info":"a\u0000b"}`)
	_, err := s.Apply(context.Background(), e)
	if err == nil || !store.IsDataError(err) {
		t.Fatalf("err = %v, want a data error", err)
	}
	if store.IsDataError(errors.New("connection refused")) {
		t.Error("outage classified as a data error")
	}
}
