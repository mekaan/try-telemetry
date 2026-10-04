// Package store holds the two tables behind the platform: append-only history
// and the latest known state per charger, connector and event type.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Event struct {
	EventID       string          `json:"event_id"`
	ChargerID     string          `json:"charger_id"`
	ConnectorID   int             `json:"connector_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	ReceivedAt    time.Time       `json:"received_at,omitzero"`
	Payload       json.RawMessage `json:"payload"`
}

// ErrConflict means an event reused an event_id we already hold for this
// charger, with different content. That is a producer bug, and silently
// calling it a duplicate would hide it.
var ErrConflict = errors.New("event_id already used for a different event on this charger")

// IsDataError reports whether Postgres refused the data itself (SQLSTATE
// class 22, data exception, or 54000, a value too large to index), as opposed
// to being unavailable. Retrying such an event can never succeed.
func IsDataError(err error) bool {
	var pg *pgconn.PgError
	if !errors.As(err, &pg) {
		return false
	}
	return strings.HasPrefix(pg.Code, "22") || pg.Code == "54000"
}

// Outcome says what Apply did with an event.
type Outcome struct {
	Duplicate    bool `json:"duplicate"`     // same event seen before, nothing written
	StateUpdated bool `json:"state_updated"` // event is now the latest for its key
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate applies the embedded migrations in file order. They are written to
// be idempotent, which is enough for a slice; production would use a
// versioned migration tool run as a separate deploy step.
func (s *Store) Migrate(ctx context.Context) error {
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := migrations.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := s.pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
	}
	return nil
}

// Apply writes an event to history and, if it is newer than what we hold,
// to latest state. Both happen in one transaction.
//
// Duplicates: the history insert does nothing on a known (charger_id,
// event_id), and we stop there after checking it really is the same event. Out of order: the latest-state upsert only overwrites when the
// incoming (occurred_at, event_id) is greater than the stored one, so an older
// event lands in history but never rolls the state back. The event_id
// tie-break makes two events with the same timestamp resolve the same way
// regardless of arrival order.
func (s *Store) Apply(ctx context.Context, e Event) (Outcome, error) {
	var out Outcome
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO telemetry_events
				(event_id, charger_id, connector_id, event_type, schema_version, occurred_at, payload)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (charger_id, event_id) DO NOTHING`,
			e.EventID, e.ChargerID, e.ConnectorID, e.EventType, e.SchemaVersion, e.OccurredAt, e.Payload)
		if err != nil {
			return fmt.Errorf("insert history: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var same bool
			err := tx.QueryRow(ctx, `
				SELECT connector_id = $3 AND event_type = $4 AND occurred_at = $5
				       AND payload = $6::jsonb AND schema_version = $7
				FROM telemetry_events WHERE charger_id = $1 AND event_id = $2`,
				e.ChargerID, e.EventID, e.ConnectorID, e.EventType, e.OccurredAt, e.Payload, e.SchemaVersion).Scan(&same)
			if err != nil {
				return fmt.Errorf("check duplicate: %w", err)
			}
			if !same {
				return ErrConflict
			}
			out.Duplicate = true
			return nil
		}

		tag, err = tx.Exec(ctx, `
			INSERT INTO latest_state
				(charger_id, connector_id, event_type, schema_version, occurred_at, event_id, payload)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (charger_id, connector_id, event_type) DO UPDATE SET
				schema_version = excluded.schema_version,
				occurred_at    = excluded.occurred_at,
				event_id       = excluded.event_id,
				payload        = excluded.payload,
				updated_at     = now()
			WHERE (excluded.occurred_at, excluded.event_id)
			    > (latest_state.occurred_at, latest_state.event_id)`,
			e.ChargerID, e.ConnectorID, e.EventType, e.SchemaVersion, e.OccurredAt, e.EventID, e.Payload)
		if err != nil {
			return fmt.Errorf("upsert latest state: %w", err)
		}
		out.StateUpdated = tag.RowsAffected() == 1
		return nil
	})
	return out, err
}

// RepairWindow is the data half of rolling back a bad release. It deletes the
// history rows received in [from, to), and for every latest-state entry that
// pointed at one of them, falls back to the newest remaining event for that
// key. State entries the release never touched are left alone; an idle
// charger's status from last week must survive the repair. Afterwards the
// ingest consumer group is reset to the release's start offset, and the fixed
// code writes those events again through the normal forward-only path.
//
// It returns how many history rows were deleted and how many state entries
// were rebuilt. A rebuilt key whose only good event is older than the hot
// window ends up with no state until its next event, or a restore from the
// S3 archive.
func (s *Store) RepairWindow(ctx context.Context, from, to time.Time) (deleted, rebuilt int, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Statements in one query share a snapshot, so the delete and the
		// rebuild have to be separate statements.
		rows, err := tx.Query(ctx, `
			WITH gone AS (
				DELETE FROM telemetry_events
				WHERE received_at >= $1 AND received_at < $2
				RETURNING charger_id, event_id
			), stale AS (
				DELETE FROM latest_state ls USING gone g
				WHERE ls.charger_id = g.charger_id AND ls.event_id = g.event_id
				RETURNING ls.charger_id, ls.connector_id, ls.event_type
			)
			SELECT (SELECT count(*) FROM gone), charger_id, connector_id, event_type FROM stale
			UNION ALL
			SELECT (SELECT count(*) FROM gone), NULL, NULL, NULL
			WHERE NOT EXISTS (SELECT 1 FROM stale)`, from, to)
		if err != nil {
			return fmt.Errorf("delete window: %w", err)
		}
		var chargers, types []string
		var connectors []int32
		for rows.Next() {
			var n int
			var charger, typ *string
			var conn *int32
			if err := rows.Scan(&n, &charger, &conn, &typ); err != nil {
				rows.Close()
				return err
			}
			deleted = n
			if charger != nil {
				chargers, connectors, types = append(chargers, *charger), append(connectors, *conn), append(types, *typ)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(chargers) == 0 {
			return nil
		}

		tag, err := tx.Exec(ctx, `
			INSERT INTO latest_state
				(charger_id, connector_id, event_type, schema_version, occurred_at, event_id, payload)
			SELECT DISTINCT ON (e.charger_id, e.connector_id, e.event_type)
				e.charger_id, e.connector_id, e.event_type, e.schema_version, e.occurred_at, e.event_id, e.payload
			FROM telemetry_events e
			JOIN unnest($1::text[], $2::int[], $3::text[]) AS k(charger_id, connector_id, event_type)
				USING (charger_id, connector_id, event_type)
			ORDER BY e.charger_id, e.connector_id, e.event_type, e.occurred_at DESC, e.event_id DESC`,
			chargers, connectors, types)
		if err != nil {
			return fmt.Errorf("rebuild state: %w", err)
		}
		rebuilt = int(tag.RowsAffected())
		return nil
	})
	return deleted, rebuilt, err
}

// StateEntry is the latest event of one type on one connector.
type StateEntry struct {
	EventID       string          `json:"event_id"`
	SchemaVersion int             `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

// ChargerState maps connector id to event type to the latest entry.
// Connector 0 is the charger itself, as in OCPP.
type ChargerState map[int]map[string]StateEntry

func (s *Store) LatestState(ctx context.Context, chargerID string) (ChargerState, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT connector_id, event_type, event_id, schema_version, occurred_at, payload
		FROM latest_state WHERE charger_id = $1`, chargerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	state := ChargerState{}
	for rows.Next() {
		var conn int
		var typ string
		var e StateEntry
		if err := rows.Scan(&conn, &typ, &e.EventID, &e.SchemaVersion, &e.OccurredAt, &e.Payload); err != nil {
			return nil, err
		}
		if state[conn] == nil {
			state[conn] = map[string]StateEntry{}
		}
		state[conn][typ] = e
	}
	return state, rows.Err()
}

// History returns the newest events for a charger, newest first.
func (s *Store) History(ctx context.Context, chargerID string, limit int) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, charger_id, connector_id, event_type, schema_version, occurred_at, received_at, payload
		FROM telemetry_events WHERE charger_id = $1
		ORDER BY occurred_at DESC, event_id DESC
		LIMIT $2`, chargerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []Event{}
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.EventID, &e.ChargerID, &e.ConnectorID, &e.EventType,
			&e.SchemaVersion, &e.OccurredAt, &e.ReceivedAt, &e.Payload); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
