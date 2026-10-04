// Package api is the versioned HTTP contract other teams build on. Teams read
// state through here (or their own Kafka consumer group), never through the
// database, so the tables stay free to change.
//
// In this slice POST /v1/events calls the ingest code directly. In production
// it would validate the envelope, produce to the Kafka topic and return 202, so
// events from team producers reach the archive and stream consumers too.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/mekaan/try-telemetry/internal/ingest"
	"github.com/mekaan/try-telemetry/internal/schema"
	"github.com/mekaan/try-telemetry/internal/store"
)

type Reader interface {
	LatestState(ctx context.Context, chargerID string) (store.ChargerState, error)
	History(ctx context.Context, chargerID string, limit int) ([]store.Event, error)
	Ping(ctx context.Context) error
}

type Server struct {
	Processor *ingest.Processor
	Reader    Reader
	Schemas   *schema.Registry
	Log       *slog.Logger
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.postEvent)
	mux.HandleFunc("GET /v1/chargers/{id}/state", s.getState)
	mux.HandleFunc("GET /v1/chargers/{id}/events", s.getEvents)
	mux.HandleFunc("GET /v1/event-types", s.getEventTypes)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.ready)
	return mux
}

func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields() // the envelope belongs to the platform; extra fields go in payload
	var env ingest.Envelope
	if err := dec.Decode(&env); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "event is larger than 1 MB")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid envelope: "+err.Error())
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid envelope: send exactly one JSON object")
		return
	}

	e, out, err := s.Processor.Process(r.Context(), env)
	switch {
	case ingest.IsInvalid(err):
		s.Log.Info("event rejected", "charger_id", e.ChargerID, "event_type", e.EventType, "reason", err.Error())
		writeError(w, http.StatusBadRequest, err.Error())
		return
	case errors.Is(err, store.ErrConflict):
		s.Log.Warn("event_id conflict", "charger_id", e.ChargerID, "event_id", e.EventID)
		writeError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		s.Log.Error("event failed", "event_id", e.EventID, "error", err)
		writeError(w, http.StatusInternalServerError, "could not store event")
		return
	}

	s.Log.Info("event processed", "event_id", e.EventID, "charger_id", e.ChargerID,
		"event_type", e.EventType, "duplicate", out.Duplicate, "state_updated", out.StateUpdated)
	status := http.StatusCreated
	if out.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"event_id":      e.EventID,
		"duplicate":     out.Duplicate,
		"state_updated": out.StateUpdated,
	})
}

func (s *Server) getState(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	state, err := s.Reader.LatestState(r.Context(), id)
	if err != nil {
		s.Log.Error("read state", "charger_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not read state")
		return
	}
	if len(state) == 0 {
		writeError(w, http.StatusNotFound, "no telemetry for charger "+id)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"charger_id": id, "connectors": state})
}

func (s *Server) getEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		limit = n
	}
	events, err := s.Reader.History(r.Context(), id, limit)
	if err != nil {
		s.Log.Error("read history", "charger_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not read history")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"charger_id": id, "events": events})
}

func (s *Server) getEventTypes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"event_types": s.Schemas.List()})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.Reader.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		slog.Default().Warn("write response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
