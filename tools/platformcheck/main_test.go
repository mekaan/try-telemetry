package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mekaan/try-telemetry/internal/schema"
)

func TestIgnoredFilesAreErrors(t *testing.T) {
	reg, err := schema.Load("../../schemas")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "billing.yml"), []byte("team: billing\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var r report
	checkConsumers(&r, reg, dir)
	if len(r.errors) == 0 || !strings.Contains(r.errors[0], "silently ignored") {
		t.Fatalf("errors = %v, want the .yml file rejected", r.errors)
	}
}

func TestDuplicateServiceAccount(t *testing.T) {
	reg, err := schema.Load("../../schemas")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	body := "team: x\nowner: \"@acme/x\"\nservice_account: x/y\naccess: stream\nevent_types: [status_notification]\n"
	for _, f := range []string{"first", "second"} {
		if err := os.WriteFile(filepath.Join(dir, f+".yaml"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var r report
	checkConsumers(&r, reg, dir)
	if !strings.Contains(strings.Join(r.errors, ";"), "already registered") {
		t.Fatalf("errors = %v, want duplicate service account rejected", r.errors)
	}
}

func TestLooseFileInSchemas(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("notes"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg, err := schema.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	var r report
	checkSchemas(&r, reg, dir)
	if !strings.Contains(strings.Join(r.errors, ";"), "only <event_type>/ directories") {
		t.Fatalf("errors = %v, want the loose file rejected", r.errors)
	}
}

func TestPIIIsBlockedOnSharedTopic(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "driver_session"), 0o755); err != nil {
		t.Fatal(err)
	}
	body := `{"x-owner": "@acme/x", "x-retention-days": 30, "x-contains-pii": true, "type": "object", "additionalProperties": false}`
	if err := os.WriteFile(filepath.Join(dir, "driver_session", "v1.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	reg, err := schema.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	var r report
	checkSchemas(&r, reg, dir)
	if !strings.Contains(strings.Join(r.errors, ";"), "personal data is not accepted") {
		t.Fatalf("errors = %v, want PII blocked", r.errors)
	}
}

func TestRepoPasses(t *testing.T) {
	reg, err := schema.Load("../../schemas")
	if err != nil {
		t.Fatal(err)
	}
	var r report
	checkSchemas(&r, reg, "../../schemas")
	checkConsumers(&r, reg, "../../consumers")
	if len(r.errors) > 0 {
		t.Fatalf("repo fails its own guardrail: %v", r.errors)
	}
}

func TestConsumerRegistrations(t *testing.T) {
	reg, err := schema.Load("../../schemas")
	if err != nil {
		t.Fatal(err)
	}
	valid := "team: x\nowner: \"@acme/x\"\nservice_account: x/y\naccess: stream\nevent_types: [status_notification]\n"
	tests := map[string]struct {
		file, body, wantErr string
	}{
		"valid":              {file: "fleet-app", body: valid},
		"reserved name":      {file: "telemetry-ingest", body: valid, wantErr: "reserved"},
		"name with dot":      {file: "billing.v2", body: valid, wantErr: "file name"},
		"unknown event type": {file: "x", body: strings.Replace(valid, "status_notification", "made_up", 1), wantErr: "unknown event type"},
		"unknown field":      {file: "x", body: valid + "database_access: true\n", wantErr: "database_access"},
		"bad access":         {file: "x", body: strings.Replace(valid, "stream", "sql", 1), wantErr: "access must be"},
		"bare service acct":  {file: "x", body: strings.Replace(valid, "x/y", "nosuch", 1), wantErr: "namespace/name"},
		"platform namespace": {file: "x", body: strings.Replace(valid, "x/y", "telemetry/telemetry-ingest", 1), wantErr: "belongs to the platform"},
		"negative rps":       {file: "x", body: valid + "rate_limit_rps: -5\n", wantErr: "rate_limit_rps"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, tt.file+".yaml"), []byte(tt.body), 0o644); err != nil {
				t.Fatal(err)
			}
			var r report
			checkConsumers(&r, reg, dir)
			got := strings.Join(r.errors, "; ")
			switch {
			case tt.wantErr == "" && got != "":
				t.Errorf("unexpected errors: %s", got)
			case tt.wantErr != "" && !strings.Contains(got, tt.wantErr):
				t.Errorf("errors = %q, want one mentioning %q", got, tt.wantErr)
			}
		})
	}
}
