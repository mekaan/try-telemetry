package schema

import (
	"strings"
	"testing"
)

const v1 = `{
  "x-owner": "@acme/charging-core", "x-retention-days": 90,
  "type": "object", "additionalProperties": false,
  "required": ["status"],
  "properties": {
    "status": {"type": "string", "enum": ["Available", "Charging"]},
    "info":   {"type": "string", "maxLength": 50}
  }
}`

func TestCompare(t *testing.T) {
	tests := []struct {
		name         string
		edit         func(string) string
		wantBreaking string
		wantReview   string
	}{
		{name: "add optional field", edit: func(s string) string {
			return strings.Replace(s, `"info":`, `"vendor": {"type": "string"}, "info":`, 1)
		}},
		{name: "add enum value", edit: func(s string) string {
			return strings.Replace(s, `"Charging"]`, `"Charging", "Faulted"]`, 1)
		}},
		{name: "remove field", wantBreaking: "payload.info: removed", edit: func(s string) string {
			return strings.Replace(s, `,
    "info":   {"type": "string", "maxLength": 50}`, "", 1)
		}},
		{name: "change type", wantBreaking: "type changed", edit: func(s string) string {
			return strings.Replace(s, `"info":   {"type": "string", "maxLength": 50}`, `"info": {"type": "integer", "maxLength": 50}`, 1)
		}},
		{name: "new required field", wantBreaking: "payload.info: became required", edit: func(s string) string {
			return strings.Replace(s, `["status"]`, `["status", "info"]`, 1)
		}},
		{name: "no longer required", wantBreaking: "payload.status: no longer required", edit: func(s string) string {
			return strings.Replace(s, `["status"]`, `[]`, 1)
		}},
		{name: "tighten constraint", wantBreaking: "maxLength changed from 50 to 20", edit: func(s string) string {
			return strings.Replace(s, `"maxLength": 50`, `"maxLength": 20`, 1)
		}},
		{name: "remove enum entirely", wantBreaking: "enum removed", edit: func(s string) string {
			return strings.Replace(s, `, "enum": ["Available", "Charging"]`, ``, 1)
		}},
		{name: "remove enum value", wantBreaking: "enum value Charging removed", edit: func(s string) string {
			return strings.Replace(s, `, "Charging"]`, `]`, 1)
		}},
		{name: "flag as PII", wantReview: "personal data", edit: func(s string) string {
			return strings.Replace(s, `"x-retention-days": 90,`, `"x-retention-days": 90, "x-contains-pii": true,`, 1)
		}},
		{name: "raise retention past limit", wantReview: "retention raised to 5000", edit: func(s string) string {
			return strings.Replace(s, `"x-retention-days": 90`, `"x-retention-days": 5000`, 1)
		}},
		{name: "lower retention", wantReview: "retention lowered from 90 to 7", edit: func(s string) string {
			return strings.Replace(s, `"x-retention-days": 90`, `"x-retention-days": 7`, 1)
		}},
		{name: "raise retention within limit", edit: func(s string) string {
			return strings.Replace(s, `"x-retention-days": 90`, `"x-retention-days": 180`, 1)
		}},
		{name: "change owner", wantReview: "ownership moves", edit: func(s string) string {
			return strings.Replace(s, "charging-core", "field-ops", 1)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := Compare([]byte(v1), []byte(tt.edit(v1)))
			if err != nil {
				t.Fatal(err)
			}
			check(t, "breaking", f.Breaking, tt.wantBreaking)
			check(t, "review", f.Review, tt.wantReview)
		})
	}
}

func TestCompareDropPII(t *testing.T) {
	withPII := strings.Replace(v1, `"x-retention-days": 90,`, `"x-retention-days": 90, "x-contains-pii": true,`, 1)
	f, err := Compare([]byte(withPII), []byte(v1))
	if err != nil {
		t.Fatal(err)
	}
	check(t, "review", f.Review, "personal-data flag removed")
}

func check(t *testing.T, kind string, got []string, want string) {
	t.Helper()
	joined := strings.Join(got, "; ")
	switch {
	case want == "" && len(got) > 0:
		t.Errorf("unexpected %s: %s", kind, joined)
	case want != "" && !strings.Contains(joined, want):
		t.Errorf("%s = %q, want it to mention %q", kind, joined, want)
	}
}

func TestCompareItemsAdded(t *testing.T) {
	withArray := strings.Replace(v1, `"info":`, `"tags": {"type": "array"}, "info":`, 1)
	withItems := strings.Replace(v1, `"info":`, `"tags": {"type": "array", "items": {"type": "integer"}}, "info":`, 1)
	f, err := Compare([]byte(withArray), []byte(withItems))
	if err != nil {
		t.Fatal(err)
	}
	check(t, "breaking", f.Breaking, "items added or removed")
}

func TestLoadRules(t *testing.T) {
	for name, tt := range map[string]struct{ body, want string }{
		"pii flag missing": {`{"x-owner": "@a/b", "x-retention-days": 30, "type": "object", "additionalProperties": false}`, "x-contains-pii is required"},
		"open root object": {`{"x-owner": "@a/b", "x-retention-days": 30, "x-contains-pii": false, "type": "object"}`, "additionalProperties"},
		"open nested object": {`{"x-owner": "@a/b", "x-retention-days": 30, "x-contains-pii": false, "type": "object", "additionalProperties": false,
			"properties": {"loc": {"type": "object"}}}`, "payload.loc: objects must set"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeSchema(t, dir, "thing", tt.body)
			if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestFormatIsEnforced(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "thing", `{"x-owner": "@a/b", "x-retention-days": 30, "x-contains-pii": false, "type": "object",
		"additionalProperties": false, "properties": {"at": {"type": "string", "format": "date-time"}}}`)
	reg, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Validate("thing", 1, []byte(`{"at": "not a date"}`)); err == nil {
		t.Error(`"not a date" accepted as date-time`)
	}
	if err := reg.Validate("thing", 1, []byte(`{"at": "2026-09-28T10:00:00Z"}`)); err != nil {
		t.Errorf("valid date-time rejected: %v", err)
	}
}

func TestLoadRequiresOwner(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "thing", `{"type": "object", "x-retention-days": 30, "x-contains-pii": false, "additionalProperties": false}`)
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "x-owner") {
		t.Fatalf("err = %v, want x-owner error", err)
	}
}

func TestLoadRejectsUncheckableKeywords(t *testing.T) {
	for name, body := range map[string]string{
		"allOf at root":    `{"x-owner": "@a/b", "x-retention-days": 30, "x-contains-pii": false, "type": "object", "additionalProperties": false, "allOf": [{"required": ["x"]}]}`,
		"$defs":            `{"x-owner": "@a/b", "x-retention-days": 30, "x-contains-pii": false, "type": "object", "additionalProperties": false, "$defs": {"s": {"maxLength": 1}}}`,
		"oneOf in field":   `{"x-owner": "@a/b", "x-retention-days": 30, "x-contains-pii": false, "type": "object", "additionalProperties": false, "properties": {"x": {"oneOf": [{"type": "string"}]}}}`,
		"x-owner in field": `{"x-owner": "@a/b", "x-retention-days": 30, "x-contains-pii": false, "type": "object", "additionalProperties": false, "properties": {"x": {"x-owner": "@c/d"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeSchema(t, dir, "thing", body)
			if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("err = %v, want unsupported keyword", err)
			}
		})
	}
}

func TestLoadRepoSchemas(t *testing.T) {
	reg, err := Load("../../schemas")
	if err != nil {
		t.Fatal(err)
	}
	if len(reg.List()) < 2 {
		t.Fatalf("loaded %d schemas", len(reg.List()))
	}
}
