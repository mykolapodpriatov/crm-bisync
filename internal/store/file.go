package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"crm-bisync/internal/clock"
)

// File is a crash-safe Store backed by an append-only log plus periodic
// snapshots. It keeps the live set in memory and uses the disk only for
// durability, which is the right trade here: the state is small (watermarks,
// link rows, hashes, a few queues) and every read is on the hot path of a
// sync decision.
//
// Durability model:
//
//   - Every mutation is appended to log.jsonl as one JSON line and fsynced
//     before the call returns. A write that returned has landed.
//   - Opening replays snapshot.json and then the log. A torn write at the tail
//     of the log, which is what a crash actually leaves behind, is discarded
//     and the file is truncated back to the last complete record.
//   - Compaction writes a new snapshot to a temp file, fsyncs it, renames it
//     over the old one and then truncates the log. The rename is atomic, so a
//     crash mid-compaction leaves either the old snapshot with the full log or
//     the new snapshot with an empty one. Both replay to the same state.
//
// SQLite and Postgres stores are a documented post-v1 want. They slot in
// behind the Store interface without the engine noticing.
type File struct {
	mu    sync.Mutex
	clock clock.Clock

	dir      string
	log      *os.File
	logBytes int64
	ops      int
	// compactAfter is how many appended ops trigger a snapshot. Zero disables
	// compaction, which the tests use to inspect the raw log.
	compactAfter int

	data   map[string]map[string]Entry
	closed bool
}

// FileOptions tunes the file store. The zero value is sensible.
type FileOptions struct {
	// Clock is the time source used to decide expiry. Nil means wall clock.
	Clock clock.Clock
	// CompactAfter is how many appended operations trigger a snapshot.
	// Zero means the default; negative disables compaction.
	CompactAfter int
}

const (
	snapshotName        = "snapshot.json"
	snapshotTempName    = "snapshot.json.tmp"
	logName             = "log.jsonl"
	defaultCompactAfter = 1000
)

// op is one record in the log. The field names are short because this file is
// written on every mutation.
type op struct {
	C string `json:"c"`
	K string `json:"k"`
	V []byte `json:"v,omitempty"`
	E int64  `json:"e,omitempty"`
	D bool   `json:"d,omitempty"`
}

// snapshot is the on-disk form of the whole live set.
type snapshot struct {
	Version int                      `json:"version"`
	Data    map[string]map[string]op `json:"data"`
}

// OpenFile opens or creates a file-backed store in dir.
func OpenFile(dir string, opts FileOptions) (*File, error) {
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	compact := opts.CompactAfter
	switch {
	case compact == 0:
		compact = defaultCompactAfter
	case compact < 0:
		compact = 0
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: create %s: %w", dir, err)
	}

	f := &File{
		clock:        opts.Clock,
		dir:          dir,
		compactAfter: compact,
		data:         make(map[string]map[string]Entry),
	}

	if err := f.loadSnapshot(); err != nil {
		return nil, err
	}
	if err := f.replayLog(); err != nil {
		return nil, err
	}

	log, err := os.OpenFile(f.path(logName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("store: open log: %w", err)
	}
	f.log = log
	return f, nil
}

func (f *File) path(name string) string { return filepath.Join(f.dir, name) }

func (f *File) loadSnapshot() error {
	raw, err := os.ReadFile(f.path(snapshotName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: read snapshot: %w", err)
	}

	var snap snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		// A snapshot is written atomically, so a broken one is not a torn
		// write, it is corruption. Refusing to start is the honest response:
		// silently continuing would resurrect stale link rows.
		return fmt.Errorf("store: snapshot is corrupt: %w", err)
	}
	for coll, entries := range snap.Data {
		for key, o := range entries {
			f.apply(coll, o)
			_ = key
		}
	}
	return nil
}

// replayLog applies the log on top of the snapshot, and truncates the file at
// the first record it cannot read. Everything after a torn record in an
// append-only log is unreachable anyway, so discarding it is what recovery
// means here rather than a heuristic.
func (f *File) replayLog() error {
	path := f.path(logName)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: open log: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var good int64
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			// A tail without a terminating newline is a torn write.
			break
		}
		if err != nil {
			return fmt.Errorf("store: read log: %w", err)
		}

		var o op
		if err := json.Unmarshal(line[:len(line)-1], &o); err != nil {
			break
		}
		f.apply(o.C, o)
		good += int64(len(line))
	}

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("store: stat log: %w", err)
	}
	if info.Size() != good {
		if err := os.Truncate(path, good); err != nil {
			return fmt.Errorf("store: truncate torn log: %w", err)
		}
	}
	f.logBytes = good
	return nil
}

// apply folds one op into the live set. It does not filter expired records:
// they stay until Sweep, and reads hide them, so a clock that moves backwards
// cannot make a record vanish permanently.
func (f *File) apply(collection string, o op) {
	if o.D {
		delete(f.data[collection], o.K)
		return
	}
	c, ok := f.data[collection]
	if !ok {
		c = make(map[string]Entry)
		f.data[collection] = c
	}
	var exp time.Time
	if o.E != 0 {
		exp = time.Unix(0, o.E).UTC()
	}
	c[o.K] = Entry{Key: o.K, Value: o.V, ExpiresAt: exp}
}

// append writes one op and fsyncs it. The caller holds the lock.
func (f *File) append(o op) error {
	line, err := json.Marshal(o)
	if err != nil {
		return fmt.Errorf("store: encode op: %w", err)
	}
	line = append(line, '\n')

	n, err := f.log.Write(line)
	f.logBytes += int64(n)
	if err != nil {
		return fmt.Errorf("store: append: %w", err)
	}
	if err := f.log.Sync(); err != nil {
		return fmt.Errorf("store: fsync: %w", err)
	}

	f.ops++
	return nil
}

// maybeCompact snapshots once enough operations have accumulated.
//
// It is deliberately separate from append and called only after apply. The
// order matters: append makes a mutation durable, apply makes it visible in
// memory, and compact serialises memory. Compacting from inside append would
// snapshot the state as it was one mutation ago and then truncate the log that
// still held it, losing exactly the record that triggered the compaction.
func (f *File) maybeCompact() error {
	if f.compactAfter > 0 && f.ops >= f.compactAfter {
		return f.compact()
	}
	return nil
}

// compact writes a snapshot and truncates the log. The caller holds the lock.
func (f *File) compact() error {
	snap := snapshot{Version: 1, Data: make(map[string]map[string]op, len(f.data))}
	for coll, entries := range f.data {
		if len(entries) == 0 {
			continue
		}
		out := make(map[string]op, len(entries))
		for key, e := range entries {
			var exp int64
			if !e.ExpiresAt.IsZero() {
				exp = e.ExpiresAt.UnixNano()
			}
			out[key] = op{C: coll, K: key, V: e.Value, E: exp}
		}
		snap.Data[coll] = out
	}

	raw, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("store: encode snapshot: %w", err)
	}

	tmp := f.path(snapshotTempName)
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("store: open temp snapshot: %w", err)
	}
	if _, err := tf.Write(raw); err != nil {
		tf.Close()
		return fmt.Errorf("store: write temp snapshot: %w", err)
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return fmt.Errorf("store: fsync temp snapshot: %w", err)
	}
	if err := tf.Close(); err != nil {
		return fmt.Errorf("store: close temp snapshot: %w", err)
	}
	// The rename is the commit point. Before it the old snapshot plus the full
	// log is the truth; after it the new snapshot is, and the log is redundant.
	if err := os.Rename(tmp, f.path(snapshotName)); err != nil {
		return fmt.Errorf("store: commit snapshot: %w", err)
	}
	if err := syncDir(f.dir); err != nil {
		return err
	}

	if err := f.log.Truncate(0); err != nil {
		return fmt.Errorf("store: truncate log: %w", err)
	}
	if _, err := f.log.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("store: rewind log: %w", err)
	}
	if err := f.log.Sync(); err != nil {
		return fmt.Errorf("store: fsync truncated log: %w", err)
	}
	f.logBytes = 0
	f.ops = 0
	return nil
}

// syncDir fsyncs a directory so a rename inside it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("store: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("store: fsync dir: %w", err)
	}
	return nil
}

// Get implements Store.
func (f *File) Get(collection, key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, false, ErrClosed
	}
	e, ok := f.data[collection][key]
	if !ok || !live(e.ExpiresAt, f.clock.Now()) {
		return nil, false, nil
	}
	return append([]byte(nil), e.Value...), true, nil
}

// Put implements Store.
func (f *File) Put(collection, key string, value []byte, expiresAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	var exp int64
	if !expiresAt.IsZero() {
		exp = expiresAt.UnixNano()
	}
	o := op{C: collection, K: key, V: append([]byte(nil), value...), E: exp}
	if err := f.append(o); err != nil {
		return err
	}
	f.apply(collection, o)
	return f.maybeCompact()
}

// Delete implements Store.
func (f *File) Delete(collection, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	if _, ok := f.data[collection][key]; !ok {
		return nil
	}
	o := op{C: collection, K: key, D: true}
	if err := f.append(o); err != nil {
		return err
	}
	f.apply(collection, o)
	return f.maybeCompact()
}

// Take implements Store.
func (f *File) Take(collection, key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, false, ErrClosed
	}
	e, ok := f.data[collection][key]
	if !ok {
		return nil, false, nil
	}
	o := op{C: collection, K: key, D: true}
	if err := f.append(o); err != nil {
		return nil, false, err
	}
	f.apply(collection, o)
	if err := f.maybeCompact(); err != nil {
		return nil, false, err
	}
	if !live(e.ExpiresAt, f.clock.Now()) {
		return nil, false, nil
	}
	return append([]byte(nil), e.Value...), true, nil
}

// List implements Store.
func (f *File) List(collection string, limit int) ([]Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, ErrClosed
	}
	now := f.clock.Now()

	keys := make([]string, 0, len(f.data[collection]))
	for k, e := range f.data[collection] {
		if live(e.ExpiresAt, now) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	out := make([]Entry, 0, len(keys))
	for _, k := range keys {
		e := f.data[collection][k]
		out = append(out, Entry{
			Key:       k,
			Value:     append([]byte(nil), e.Value...),
			ExpiresAt: e.ExpiresAt,
		})
	}
	return out, nil
}

// Len implements Store.
func (f *File) Len(collection string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, ErrClosed
	}
	now := f.clock.Now()
	n := 0
	for _, e := range f.data[collection] {
		if live(e.ExpiresAt, now) {
			n++
		}
	}
	return n, nil
}

// Sweep implements Store.
func (f *File) Sweep(now time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, ErrClosed
	}

	type ref struct{ coll, key string }
	var dead []ref
	for coll, entries := range f.data {
		for key, e := range entries {
			if !live(e.ExpiresAt, now) {
				dead = append(dead, ref{coll, key})
			}
		}
	}
	sort.Slice(dead, func(i, j int) bool {
		if dead[i].coll != dead[j].coll {
			return dead[i].coll < dead[j].coll
		}
		return dead[i].key < dead[j].key
	})

	for _, d := range dead {
		o := op{C: d.coll, K: d.key, D: true}
		if err := f.append(o); err != nil {
			return 0, err
		}
		f.apply(d.coll, o)
	}
	if err := f.maybeCompact(); err != nil {
		return 0, err
	}
	return len(dead), nil
}

// Close implements Store.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	f.data = nil
	if f.log == nil {
		return nil
	}
	err := f.log.Close()
	f.log = nil
	if err != nil {
		return fmt.Errorf("store: close log: %w", err)
	}
	return nil
}

// Compact forces a snapshot. The engine calls it on graceful shutdown so a
// restart replays a short log rather than a long one.
func (f *File) Compact() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	return f.compact()
}

var _ Store = (*File)(nil)
