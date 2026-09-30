package updater

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCoreBaseName(t *testing.T) {
	fmt.Println("Core base name =", DefaultCoreUpdater.CoreBaseName())
}

func TestTheNewCoreMustStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the fake cores are shell scripts")
	}
	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
		return path
	}
	u := &CoreUpdater{}
	if err := u.probeCore(write("good", "#!/bin/sh\necho 'Mihomo Meta v1.19.32 linux amd64 with go1.24'\n")); err != nil {
		t.Fatalf("a core that starts: %v", err)
	}
	for name, body := range map[string]string{
		"exits":   "#!/bin/sh\necho 'This program can only be run on processors supporting x86-64-v3'\nexit 1\n",
		"garbage": "\x7fELF\x00\x00broken",
		"other":   "#!/bin/sh\necho hello\n",
	} {
		if err := u.probeCore(write(name, body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
