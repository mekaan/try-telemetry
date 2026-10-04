// Package ingest turns a raw telemetry event into stored history and state.
// Process does not know about transport: the HTTP handler calls it in this
// slice, and a Kafka consumer would call the same function in production.
package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mekaan/try-telemetry/internal/schema"
	"github.com/mekaan/try-telemetry/internal/store"
)

// MaxClockSkew bounds how far in the future occurred_at may be. Without it, one
// charger with a clock set to 2031 would pin its latest state until 2031,
// because nothing could ever be newer.
const MaxClockSkew = 5 * time.Minute

// MaxEventAge matches the hot history window in Postgres. A charger that was
// offline longer than this sends its backlog to the dead-letter topic for a
// deliberate backfill, instead of into a partition that no longer exists.
const MaxEventAge = 7 * 24 * time.Hour

// MaxConnectorID is far above any real charger and far below what would
// overflow the integer column.
const MaxConnectorID = 1000

// Ids are bounded and plain, so they fit in an index row and never reach
// Postgres with bytes it refuses. Anything the database could reject has to
// be caught here: a consumer can dead-letter an invalid event, but it would
// retry a database error forever and hold the partition.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// Envelope is the part of an event the platform owns. Producers send this;
// everything team-specific goes in Payload. Unknown fields are rejected, and
// server-side fields like received_at are not part of it.
type Envelope struct {
	EventID       string          `json:"event_id"`
	ChargerID     string          `json:"charger_id"`
	ConnectorID   *int            `json:"connector_id"` // pointer: missing must not silently mean 0, the charger itself
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

// InvalidError marks events the producer must fix. In production these go to
// a dead-letter topic for the owning team instead of being retried.
type InvalidError struct{ msg string }

func (e *InvalidError) Error() string { return e.msg }

func invalid(format string, args ...any) error {
	return &InvalidError{msg: fmt.Sprintf(format, args...)}
}

func IsInvalid(err error) bool {
	var ie *InvalidError
	return errors.As(err, &ie)
}

// IsPermanent reports whether retrying can never succeed. A Kafka consumer
// sends these to the dead-letter topic with an alert to the event type's
// owner; anything else (the database being down) it retries, holding the
// partition.
func IsPermanent(err error) bool {
	return IsInvalid(err) || errors.Is(err, store.ErrConflict)
}

type Writer interface {
	Apply(ctx context.Context, e store.Event) (store.Outcome, error)
}

type Processor struct {
	Schemas *schema.Registry
	Store   Writer
	Now     func() time.Time
}

func (p *Processor) Process(ctx context.Context, env Envelope) (store.Event, store.Outcome, error) {
	e, err := p.normalise(env)
	if err != nil {
		return e, store.Outcome{}, err
	}
	if err := p.Schemas.Validate(e.EventType, e.SchemaVersion, e.Payload); err != nil {
		return e, store.Outcome{}, invalid("%v", err)
	}
	if err := checkNUL(e.Payload); err != nil {
		return e, store.Outcome{}, err
	}
	out, err := p.Store.Apply(ctx, e)
	if store.IsDataError(err) {
		// A backstop for anything the checks above missed: the database
		// refused the data itself, so retrying can't help.
		return e, out, invalid("rejected by storage: %v", err)
	}
	return e, out, err
}

// checkNUL rejects NUL characters anywhere in the payload's strings or keys.
// JSON allows them; Postgres jsonb does not.
func checkNUL(payload json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return invalid("payload is not valid JSON: %v", err)
	}
	var walk func(any) bool
	walk = func(v any) bool {
		switch t := v.(type) {
		case string:
			return strings.ContainsRune(t, 0)
		case []any:
			for _, x := range t {
				if walk(x) {
					return true
				}
			}
		case map[string]any:
			for k, x := range t {
				if strings.ContainsRune(k, 0) || walk(x) {
					return true
				}
			}
		}
		return false
	}
	if walk(v) {
		return invalid("payload contains a NUL character, which can't be stored")
	}
	return nil
}

func (p *Processor) normalise(env Envelope) (store.Event, error) {
	e := store.Event{
		EventID: env.EventID, ChargerID: env.ChargerID, EventType: env.EventType,
		SchemaVersion: env.SchemaVersion, OccurredAt: env.OccurredAt, Payload: env.Payload,
	}
	switch {
	case env.ChargerID == "":
		return e, invalid("charger_id is required")
	case !idPattern.MatchString(env.ChargerID):
		return e, invalid("charger_id must be 1 to 128 letters, digits or . _ : -")
	case env.EventID != "" && !idPattern.MatchString(env.EventID):
		return e, invalid("event_id must be 1 to 128 letters, digits or . _ : -")
	case env.EventType == "":
		return e, invalid("event_type is required")
	case env.ConnectorID == nil:
		return e, invalid("connector_id is required (0 for the charger itself)")
	case *env.ConnectorID < 0 || *env.ConnectorID > MaxConnectorID:
		return e, invalid("connector_id must be 0 (the charger) or a connector number up to %d", MaxConnectorID)
	case env.OccurredAt.IsZero():
		return e, invalid("occurred_at is required")
	case len(env.Payload) == 0:
		return e, invalid("payload is required")
	}
	e.ConnectorID = *env.ConnectorID

	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	if e.OccurredAt.After(now().Add(MaxClockSkew)) {
		return e, invalid("occurred_at %s is in the future; check the charger clock", e.OccurredAt.Format(time.RFC3339))
	}
	if e.OccurredAt.Before(now().Add(-MaxEventAge)) {
		return e, invalid("occurred_at %s is older than %d days; send it through a backfill instead", e.OccurredAt.Format(time.RFC3339), int(MaxEventAge.Hours()/24))
	}
	// Postgres stores microseconds. Truncating first means the id we derive
	// and the timestamp we store describe the same instant.
	e.OccurredAt = e.OccurredAt.UTC().Truncate(time.Microsecond)
	if e.SchemaVersion == 0 {
		e.SchemaVersion = 1
	}
	if e.EventID == "" {
		id, err := DeriveID(e)
		if err != nil {
			return e, invalid("payload is not valid JSON: %v", err)
		}
		e.EventID = id
	}
	return e, nil
}

// DeriveID gives events without a producer id a deterministic one, so a retry
// of the same reading dedupes. It covers the payload as well as charger,
// connector, type and time: OCPP timestamps are often whole seconds, and two
// different status changes in the same second are two events, not one.
// The payload is canonicalised first, so key order and whitespace don't
// change the id.
func DeriveID(e store.Event) (string, error) {
	// UseNumber keeps numbers exact; float64 would give 2^53 and 2^53+1 the
	// same id while Postgres sees two different payloads.
	dec := json.NewDecoder(bytes.NewReader(e.Payload))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(v) // map keys come out sorted
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, part := range []string{
		e.ChargerID,
		strconv.Itoa(e.ConnectorID),
		e.EventType,
		e.OccurredAt.UTC().Format(time.RFC3339Nano),
		string(canonical),
	} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return "d_" + hex.EncodeToString(h.Sum(nil))[:32], nil
}
