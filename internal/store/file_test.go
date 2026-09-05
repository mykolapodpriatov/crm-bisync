package store_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/store"
)

// The on-disk names, repeated here on purpose: these tests are about the file
// format, so they should fail if it changes silently.
const (
	logFile      = "log.jsonl"
	snapshotFile = "snapshot.json"
)

func openFile(t *testing.T, dir string, c clock.Clock, compactAfter int) *store.File {
	t.Helper()
	s, err := store.OpenFile(dir, store.FileOptions{Clock: c, CompactAfter: compactAfter})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	return s
}

func TestFileSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewManual(epoch)

	s := openFile(t, dir, c, -1)
	ty := store.NewTyped(s)
	if err := ty.SetWatermark("hubspot", "contact", epoch); err != nil {
		t.Fatalf("SetWatermark: %v", err)
	}
	left := store.Ref{Connector: "hubspot", Kind: "contact", RemoteID: "1"}
	right := store.Ref{Connector: "twenty", Kind: "contact", RemoteID: "a"}
	if err := ty.PutLink(store.Link{Left: left, Right: right, LinkedAt: epoch}); err != nil {
		t.Fatalf("PutLink: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := openFile(t, dir, c, -1)
	defer s2.Close()
	ty2 := store.NewTyped(s2)

	got, ok, err := ty2.Watermark("hubspot", "contact")
	if err != nil || !ok || !got.Equal(epoch) {
		t.Fatalf("watermark after reopen = %v,%v,%v", got, ok, err)
	}
	if _, ok, _ := ty2.GetLink(right); !ok {
		t.Fatal("link did not survive reopen")
	}
}

// A delete has to be durable too, otherwise replaying the log resurrects a
// consumed origin entry and the engine suppresses a real change.
func TestFileDeletesAreDurable(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewManual(epoch)

	s := openFile(t, dir, c, -1)
	if err := s.Put(store.CollOrigins, "tag", []byte("v"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok, err := s.Take(store.CollOrigins, "tag"); !ok || err != nil {
		t.Fatalf("Take = %v,%v", ok, err)
	}
	s.Close()

	s2 := openFile(t, dir, c, -1)
	defer s2.Close()
	if _, ok, _ := s2.Get(store.CollOrigins, "tag"); ok {
		t.Fatal("a taken entry came back after reopen")
	}
}

func TestFileExpirySurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewManual(epoch)

	s := openFile(t, dir, c, -1)
	if err := s.Put(store.CollOrigins, "tag", []byte("v"), epoch.Add(time.Minute)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	s.Close()

	c2 := clock.NewManual(epoch.Add(2 * time.Minute))
	s2 := openFile(t, dir, c2, -1)
	defer s2.Close()
	if _, ok, _ := s2.Get(store.CollOrigins, "tag"); ok {
		t.Fatal("an expired entry was visible after reopen")
	}
}

// This is what a crash actually leaves behind: a final record that was only
// half written. Everything before it must survive, the torn record must not,
// and the file must be repaired rather than left to trip the next open.
func TestFileRecoversFromATornTail(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewManual(epoch)

	s := openFile(t, dir, c, -1)
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Put(store.CollLinks, k, []byte(k), time.Time{}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	s.Close()

	path := filepath.Join(dir, logFile)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	// A record that started to be written and never finished: valid JSON
	// prefix, no terminating newline.
	if _, err := f.WriteString(`{"c":"links","k":"d","v":"ZA`); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	f.Close()

	s2 := openFile(t, dir, c, -1)
	defer s2.Close()

	for _, k := range []string{"a", "b", "c"} {
		if _, ok, _ := s2.Get(store.CollLinks, k); !ok {
			t.Fatalf("record %q was lost recovering from a torn tail", k)
		}
	}
	if _, ok, _ := s2.Get(store.CollLinks, "d"); ok {
		t.Fatal("a torn record was applied")
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("log was not truncated back to the last complete record: %d, want %d",
			after.Size(), before.Size())
	}
}

// A whole line that is not valid JSON is the same class of damage, and the
// same answer: nothing after the first unreadable byte is trustworthy.
func TestFileStopsAtAGarbageRecord(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewManual(epoch)

	s := openFile(t, dir, c, -1)
	if err := s.Put(store.CollLinks, "a", []byte("a"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	s.Close()

	f, err := os.OpenFile(filepath.Join(dir, logFile), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := f.WriteString("not json at all\n" + `{"c":"links","k":"z"}` + "\n"); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	f.Close()

	s2 := openFile(t, dir, c, -1)
	defer s2.Close()
	if _, ok, _ := s2.Get(store.CollLinks, "a"); !ok {
		t.Fatal("the record before the garbage was lost")
	}
	if _, ok, _ := s2.Get(store.CollLinks, "z"); ok {
		t.Fatal("a record after the garbage was applied")
	}
}

func TestFileCompactsAndReplaysTheSnapshot(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewManual(epoch)

	s := openFile(t, dir, c, 5)
	for i := 0; i < 5; i++ {
		if err := s.Put(store.CollLinks, string(rune('a'+i)), []byte("v"), time.Time{}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	if _, err := os.Stat(filepath.Join(dir, snapshotFile)); err != nil {
		t.Fatalf("no snapshot after the compaction threshold: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, logFile))
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("log is %d bytes after compaction, want 0", info.Size())
	}
	s.Close()

	s2 := openFile(t, dir, c, -1)
	defer s2.Close()
	for i := 0; i < 5; i++ {
		if _, ok, _ := s2.Get(store.CollLinks, string(rune('a'+i))); !ok {
			t.Fatalf("record %c was lost across compaction", rune('a'+i))
		}
	}
}

// The state after a compaction is a snapshot plus whatever was written since.
// Opening has to apply both, in that order.
func TestFileReplaysSnapshotThenLog(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewManual(epoch)

	s := openFile(t, dir, c, -1)
	if err := s.Put(store.CollLinks, "old", []byte("v1"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Put(store.CollLinks, "new", []byte("v2"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// A value written before the snapshot and overwritten after it must end up
	// with the later value, which is what "snapshot then log" buys.
	if err := s.Put(store.CollLinks, "old", []byte("v3"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	s.Close()

	s2 := openFile(t, dir, c, -1)
	defer s2.Close()
	if v, ok, _ := s2.Get(store.CollLinks, "old"); !ok || string(v) != "v3" {
		t.Fatalf("old = %q,%v, want v3,true", v, ok)
	}
	if v, ok, _ := s2.Get(store.CollLinks, "new"); !ok || string(v) != "v2" {
		t.Fatalf("new = %q,%v, want v2,true", v, ok)
	}
}

// A snapshot is written to a temp file and renamed, so it is either wholly
// there or not there at all. A corrupt one therefore means real corruption,
// and starting anyway would resurrect stale link rows.
func TestFileRefusesACorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewManual(epoch)

	s := openFile(t, dir, c, -1)
	if err := s.Put(store.CollLinks, "a", []byte("v"), time.Time{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s.Close()

	if err := os.WriteFile(filepath.Join(dir, snapshotFile), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}
	if _, err := store.OpenFile(dir, store.FileOptions{Clock: c}); err == nil {
		t.Fatal("OpenFile succeeded on a corrupt snapshot")
	}
}

func TestFileCreatesItsDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "state")
	s, err := store.OpenFile(dir, store.FileOptions{Clock: clock.NewManual(epoch)})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer s.Close()
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("directory was not created: %v", err)
	}
}
