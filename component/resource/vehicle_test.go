package resource

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSafeWriteReplacesTheFileWholeAndLeavesNoTemporary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "rules.txt")

	if err := safeWrite(path, []byte("old")); err != nil {
		t.Fatal(err)
	}
	// A stale temporary from a killed write must not get in the way.
	if err := os.WriteFile(path+".tmp", []byte("half"), fileMode); err != nil {
		t.Fatal(err)
	}
	if err := safeWrite(path, []byte("new")); err != nil {
		t.Fatal(err)
	}

	if got, err := os.ReadFile(path); err != nil || string(got) != "new" {
		t.Fatalf("got %q, %v", got, err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary left behind: %v", err)
	}
}

func TestSafeWriteWritesThroughASymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "shared.dat")
	link := filepath.Join(dir, "GeoSite.dat")
	if err := os.WriteFile(target, []byte("old"), fileMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skip("symlinks unavailable:", err)
	}

	if err := safeWrite(link, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("the link was replaced: %v", err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "new" {
		t.Fatalf("target got %q, %v", got, err)
	}
}

func TestSafeWriteFallsBackToWritingInPlace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.txt")
	// The temporary cannot be created: its name is taken by a directory that
	// cannot be removed either.
	if err := os.MkdirAll(filepath.Join(path+".tmp", "busy"), dirMode); err != nil {
		t.Fatal(err)
	}

	if err := safeWrite(path, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "new" {
		t.Fatalf("got %q, %v", got, err)
	}
}
