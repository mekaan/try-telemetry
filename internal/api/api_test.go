package api_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mekaan/try-telemetry/internal/api"
	"github.com/mekaan/try-telemetry/internal/ingest"
	"github.com/mekaan/try-telemetry/internal/schema"
	"github.com/mekaan/try-telemetry/internal/store"
)

// fake keeps the HTTP contract testable without Postgres; the SQL has its
// own tests in internal/store.
type fake struct{ events map[string]store.Event }

func (f *fake) Apply(_ context.Context, e store.Event) (store.Outcome, error) {
	key := e.ChargerID + "/" + e.EventID
	if old, ok := f.events[key]; ok {
		if !bytes.Equal(old.Payload, e.Payload) {
			return store.Outcome{}, store.ErrConflict
		}
		return store.Outcome{Duplicate: true}, nil
	}
	f.events[key] = e
	return store.Outcome{StateUpdated: true}, nil
}

func (f *fake) LatestState(_ context.Context, id string) (store.ChargerState, error) {
	state := store.ChargerState{}
	for _, e := range f.events {
		if e.ChargerID == id {
			state[e.ConnectorID] = map[string]store.StateEntry{e.EventType: {EventID: e.EventID, Payload: e.Payload}}
		}
	}
	return state, nil
}

func (f *fake) History(context.Context, string, int) ([]store.Event, error) { return nil, nil }
func (f *fake) Ping(context.Context) error                                  { return nil }

func server(t *testing.T) http.Handler {
	t.Helper()
	reg, err := schema.Load("../../schemas")
	if err != nil {
		t.Fatal(err)
	}
	f := &fake{events: map[string]store.Event{}}
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	return (&api.Server{
		Processor: &ingest.Processor{Schemas: reg, Store: f, Now: func() time.Time { return now }},
		Reader:    f,
		Schemas:   reg,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}).Routes()
}

const ok = `{"event_id":"e1","charger_id":"DK-1","connector_id":1,"event_type":"status_notification",
  "occurred_at":"2026-09-28T11:59:00Z","payload":{"status":"Charging","error_code":"NoError"}}`

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
	return rec
}

func TestPostEvent(t *testing.T) {
	tests := []struct {
		name  string
		first string // sent before the request under test, if set
		body  string
		want  int
	}{
		{name: "new event", body: ok, want: http.StatusCreated},
		{name: "duplicate", first: ok, body: ok, want: http.StatusOK},
		{name: "same id, other content", first: ok,
			body: `{"event_id":"e1","charger_id":"DK-1","connector_id":1,"event_type":"status_notification",
			  "occurred_at":"2026-09-28T11:59:00Z","payload":{"status":"Faulted","error_code":"NoError"}}`,
			want: http.StatusConflict},
		{name: "missing connector_id",
			body: `{"charger_id":"DK-1","event_type":"status_notification","occurred_at":"2026-09-28T11:59:00Z",
			  "payload":{"status":"Charging","error_code":"NoError"}}`,
			want: http.StatusBadRequest},
		{name: "server field in envelope",
			body: `{"charger_id":"DK-1","connector_id":1,"event_type":"status_notification","occurred_at":"2026-09-28T11:59:00Z",
			  "received_at":"2020-01-01T00:00:00Z","payload":{"status":"Charging","error_code":"NoError"}}`,
			want: http.StatusBadRequest},
		{name: "trailing data", body: ok + ` {"x":1}`, want: http.StatusBadRequest},
		{name: "not json", body: `status=Charging`, want: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := server(t)
			if tt.first != "" {
				do(h, "POST", "/v1/events", tt.first)
			}
			if rec := do(h, "POST", "/v1/events", tt.body); rec.Code != tt.want {
				t.Errorf("status = %d, want %d: %s", rec.Code, tt.want, rec.Body)
			}
		})
	}
}

// Reproduces a bug found in review: OCPP timestamps are often whole seconds,
// and two status changes in the same second without event ids used to get
// the same derived id, so the second was rejected as a conflict.
func TestSameSecondWithoutIDs(t *testing.T) {
	h := server(t)
	for _, status := range []string{"Finishing", "Available"} {
		body := `{"charger_id":"DK-1","connector_id":1,"event_type":"status_notification",
		  "occurred_at":"2026-09-28T11:59:00Z","payload":{"status":"` + status + `","error_code":"NoError"}}`
		if rec := do(h, "POST", "/v1/events", body); rec.Code != http.StatusCreated {
			t.Errorf("%s: status = %d, want 201: %s", status, rec.Code, rec.Body)
		}
	}
}

func TestTooLarge(t *testing.T) {
	body := `{"charger_id":"DK-1","payload":"` + strings.Repeat("x", 2<<20) + `"}`
	if rec := do(server(t), "POST", "/v1/events", body); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

func TestReads(t *testing.T) {
	h := server(t)
	do(h, "POST", "/v1/events", ok)
	for path, want := range map[string]int{
		"/v1/chargers/DK-1/state":             http.StatusOK,
		"/v1/chargers/unknown/state":          http.StatusNotFound,
		"/v1/chargers/DK-1/events?limit=10":   http.StatusOK,
		"/v1/chargers/DK-1/events?limit=5000": http.StatusBadRequest,
		"/v1/event-types":                     http.StatusOK,
	} {
		if rec := do(h, "GET", path, ""); rec.Code != want {
			t.Errorf("GET %s = %d, want %d", path, rec.Code, want)
		}
	}
}
