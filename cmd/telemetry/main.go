// Command telemetry runs the ingest path and the read API in one process.
// In production they would be two deployments of the same image (a Kafka
// consumer and an HTTP API) so they scale and fail independently.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mekaan/try-telemetry/internal/api"
	"github.com/mekaan/try-telemetry/internal/ingest"
	"github.com/mekaan/try-telemetry/internal/schema"
	"github.com/mekaan/try-telemetry/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("exiting", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	schemas, err := schema.Load(env("SCHEMA_DIR", "schemas"))
	if err != nil {
		return err
	}
	for _, s := range schemas.List() {
		log.Info("schema loaded", "event_type", s.Type, "schema_version", s.Version, "owner", s.Owner)
	}

	db, err := store.Open(ctx, env("DATABASE_URL", "postgres://telemetry:telemetry@localhost:5432/telemetry"))
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		return err
	}

	srv := &http.Server{
		Addr: env("ADDR", ":8080"),
		Handler: (&api.Server{
			Processor: &ingest.Processor{Schemas: schemas, Store: db},
			Reader:    db,
			Schemas:   schemas,
			Log:       log,
		}).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
