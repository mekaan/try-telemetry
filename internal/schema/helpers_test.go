package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func writeSchema(t *testing.T, dir, eventType, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, eventType), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, eventType, "v1.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
