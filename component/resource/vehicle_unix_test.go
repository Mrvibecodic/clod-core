//go:build !windows

package resource

import (
	"os"
	"path/filepath"
	"testing"
)

// A reader that opened the file before the write keeps reading the old data
// whole: the new data arrives as a new file, the old one is never rewritten.
func TestSafeWriteNeverRewritesTheOldFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.txt")
	if err := os.WriteFile(path, []byte("old rules"), fileMode); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if err := safeWrite(path, []byte("new rules")); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, 9)
	if _, err := reader.ReadAt(got, 0); err != nil || string(got) != "old rules" {
		t.Fatalf("the old file was rewritten: %q, %v", got, err)
	}
	if now, err := os.ReadFile(path); err != nil || string(now) != "new rules" {
		t.Fatalf("got %q, %v", now, err)
	}
}
