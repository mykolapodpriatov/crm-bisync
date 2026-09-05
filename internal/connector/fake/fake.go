// Package fake is an in-memory CRM that can misbehave on demand.
//
// It is not a test helper, it is infrastructure. Every correctness claim this
// repository makes is demonstrated against it, so it has to be able to fail in
// the ways real CRMs fail: redelivering webhooks, delivering them out of order
// or too late, returning 429 mid-batch, reporting coarse or skewed timestamps,
// renaming a field underneath a running mapping, and rejecting a write for a
// validation rule that lives in the customer's account rather than in our
// config.
package fake

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/connector"
	"crm-bisync/internal/model"
)

// Header names the fake signs with. They mirror the shape every real CRM uses:
// a signature over the timestamp and the body, so a replayed body with an old
// timestamp fails even though its signature is genuine.
const (
	HeaderSignature = "X-Fake-Signature"
	HeaderTimestamp = "X-Fake-Timestamp"
	HeaderDelivery  = "X-Fake-Delivery"

	// SignatureWindow is how old a delivery may be. Real CRMs use five
	// minutes, and so does this.
	SignatureWindow = 5 * time.Minute
)

// Faults is the misbehaviour this instance exhibits. The zero value is a
// well-behaved CRM, which no real CRM is.
type Faults struct {
	// DuplicateWebhooks delivers every event twice.
	DuplicateWebhooks bool
	// ShuffleWebhooks emits pending deliveries in a seeded random order.
	ShuffleWebhooks bool
	// WebhookDelay holds deliveries back, so they can arrive after the
	// engine's echo window has closed.
	WebhookDelay time.Duration
	// NotificationOnly strips the record body from webhook events, forcing
	// the engine to re-fetch. Several real CRMs only ever do this.
	NotificationOnly bool

	// RateLimitEvery makes every Nth call return KindRateLimited. Zero is off.
	RateLimitEvery int
	// RateLimitRetryAfter is what the peer reports. Zero means it says
	// nothing, which is the case the engine's backoff has to cover.
	RateLimitRetryAfter time.Duration

	// ServerErrorEvery makes every Nth call return KindTransient. Zero is off.
	ServerErrorEvery int
	// ServerErrorRate is the seeded probability of a transient failure on any
	// call, for the chaos harness. Deterministic for a given seed.
	ServerErrorRate float64

	// TimestampGranularity rounds reported modification times down. One
	// second is common and is enough to make a naive watermark skip records.
	TimestampGranularity time.Duration
	// ClockOffset skews this peer's clock against the engine's.
	ClockOffset time.Duration

	// PageSize caps a delta read. Zero means one page.
	PageSize int

	// RenameField renames a field on the schema and on every record after
	// RenameAfter calls to Describe, simulating an admin editing the CRM
	// while the sync is running.
	RenameField struct {
		Kind       string
		From, To   string
		AfterCalls int
	}

	// RejectField and RejectValue simulate an account-level validation rule:
	// a write carrying that value is permanently rejected, not retried. This
	// is the shape of HubSpot enforcing admin validation on write paths.
	RejectField string
	RejectValue string
}

// Options configures a fake connector.
type Options struct {
	Name    string
	Clock   clock.Clock
	Caps    connector.Caps
	Secret  string
	Schemas map[string]connector.Schema
	Faults  Faults
	Seed    int64
}

type record struct {
	id        string
	fields    map[string]any
	updatedAt time.Time
	version   int
	deleted   bool
}

type pending struct {
	event   model.ChangeEvent
	dueAt   time.Time
	seq     uint64
	delivID string
}

// Fake is an in-memory CRM.
type Fake struct {
	mu    sync.Mutex
	name  string
	clock clock.Clock
	caps  connector.Caps
	sec   []byte
	f     Faults
	rnd   *rand.Rand

	schemas map[string]connector.Schema
	data    map[string]map[string]*record
	// idem maps an idempotency key to the write it produced, which is what
	// Caps.NativeIdempotency means for this peer.
	idem map[string]connector.WriteResult

	queue  []pending
	nextID int
	seq    uint64

	calls        map[string]int
	describeSeen int
	renamed      bool
}

// New builds a fake connector. A nil clock means the wall clock.
func New(opts Options) *Fake {
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	if opts.Name == "" {
		opts.Name = "fake"
	}
	if opts.Secret == "" {
		opts.Secret = "fake-secret"
	}
	f := &Fake{
		name:    opts.Name,
		clock:   opts.Clock,
		caps:    opts.Caps,
		sec:     []byte(opts.Secret),
		f:       opts.Faults,
		rnd:     rand.New(rand.NewSource(opts.Seed)),
		schemas: make(map[string]connector.Schema, len(opts.Schemas)),
		data:    make(map[string]map[string]*record),
		idem:    make(map[string]connector.WriteResult),
		calls:   make(map[string]int),
	}
	for k, s := range opts.Schemas {
		f.schemas[k] = s
	}
	return f
}

// Name implements connector.Connector.
func (f *Fake) Name() string { return f.name }

// Capabilities implements connector.Connector.
func (f *Fake) Capabilities() connector.Caps { return f.caps }

// now is this peer's clock, which is not the engine's.
func (f *Fake) now() time.Time { return f.clock.Now().Add(f.f.ClockOffset) }

// stamp is how this peer reports a modification time: its own clock, rounded
// down by whatever granularity it keeps.
func (f *Fake) stamp() time.Time {
	t := f.now()
	if f.f.TimestampGranularity > 0 {
		t = t.Truncate(f.f.TimestampGranularity)
	}
	return t.UTC()
}

// gate applies the injected transport faults. The caller holds the lock.
func (f *Fake) gate(op string) error {
	f.calls[op]++
	n := f.calls[op]

	if f.f.RateLimitEvery > 0 && n%f.f.RateLimitEvery == 0 {
		return &connector.Error{
			Connector: f.name, Op: op, Kind: connector.KindRateLimited,
			RetryAfter: f.f.RateLimitRetryAfter,
			Err:        fmt.Errorf("too many requests"),
		}
	}
	if f.f.ServerErrorEvery > 0 && n%f.f.ServerErrorEvery == 0 {
		return connector.Errorf(f.name, op, connector.KindTransient, "upstream 503")
	}
	if f.f.ServerErrorRate > 0 && f.rnd.Float64() < f.f.ServerErrorRate {
		return connector.Errorf(f.name, op, connector.KindTransient, "upstream 500")
	}
	return nil
}

// Calls reports how many times an operation was attempted, including the
// attempts that were failed on purpose. Tests use it to prove the engine did
// not make a request it should have skipped.
func (f *Fake) Calls(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[op]
}

// Describe implements connector.Connector.
func (f *Fake) Describe(ctx context.Context, kind string) (connector.Schema, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return connector.Schema{}, err
	}
	if err := f.gate("describe"); err != nil {
		return connector.Schema{}, err
	}

	f.describeSeen++
	f.maybeRename()

	s, ok := f.schemas[kind]
	if !ok {
		return connector.Schema{}, connector.Errorf(f.name, "describe",
			connector.KindNotFound, "no object %q", kind)
	}
	out := connector.Schema{Kind: s.Kind, Fields: append([]connector.FieldSpec(nil), s.Fields...)}
	return out, nil
}

// maybeRename applies the configured schema drift once the trigger is reached.
// The caller holds the lock.
func (f *Fake) maybeRename() {
	r := f.f.RenameField
	if f.renamed || r.From == "" || f.describeSeen <= r.AfterCalls {
		return
	}
	f.renamed = true

	s := f.schemas[r.Kind]
	for i := range s.Fields {
		if s.Fields[i].Name == r.From {
			s.Fields[i].Name = r.To
		}
	}
	f.schemas[r.Kind] = s

	for _, rec := range f.data[r.Kind] {
		if v, ok := rec.fields[r.From]; ok {
			delete(rec.fields, r.From)
			rec.fields[r.To] = v
		}
	}
}

// ListChanged implements connector.Connector.
func (f *Fake) ListChanged(ctx context.Context, kind string, since time.Time, cursor string) (connector.Page, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return connector.Page{}, err
	}
	if err := f.gate("list"); err != nil {
		return connector.Page{}, err
	}

	var live []*record
	var deleted []string
	for _, rec := range f.data[kind] {
		if rec.updatedAt.Before(since) {
			continue
		}
		if rec.deleted {
			// Only a peer that soft deletes can report a deletion on a poll.
			if f.caps.SoftDelete {
				deleted = append(deleted, rec.id)
			}
			continue
		}
		live = append(live, rec)
	}
	// Sort by (updatedAt, id) so paging is stable and a cursor means something.
	sort.Slice(live, func(i, j int) bool {
		if !live[i].updatedAt.Equal(live[j].updatedAt) {
			return live[i].updatedAt.Before(live[j].updatedAt)
		}
		return live[i].id < live[j].id
	})
	sort.Strings(deleted)

	start := 0
	if cursor != "" {
		n, err := strconv.Atoi(cursor)
		if err != nil {
			return connector.Page{}, connector.Errorf(f.name, "list",
				connector.KindPermanent, "bad cursor %q", cursor)
		}
		start = n
	}
	if start > len(live) {
		start = len(live)
	}

	end := len(live)
	next := ""
	if f.f.PageSize > 0 && end-start > f.f.PageSize {
		end = start + f.f.PageSize
		next = strconv.Itoa(end)
	}

	page := connector.Page{Cursor: next}
	for _, rec := range live[start:end] {
		page.Records = append(page.Records, f.toModel(kind, rec))
	}
	// Deletions ride along with the first page only, so the engine sees each
	// one once per poll rather than once per page.
	if start == 0 {
		page.Deleted = deleted
	}
	return page, nil
}

// Get implements connector.Connector.
func (f *Fake) Get(ctx context.Context, kind, remoteID string) (model.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return model.Record{}, err
	}
	if err := f.gate("get"); err != nil {
		return model.Record{}, err
	}
	rec, ok := f.data[kind][remoteID]
	if !ok || rec.deleted {
		return model.Record{}, connector.Errorf(f.name, "get",
			connector.KindNotFound, "no %s %q", kind, remoteID)
	}
	return f.toModel(kind, rec), nil
}

// Upsert implements connector.Connector.
func (f *Fake) Upsert(ctx context.Context, kind string, rec model.Record, key string) (connector.WriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return connector.WriteResult{}, err
	}

	// The idempotency check comes before the fault gate on purpose. A peer
	// that honours the key short-circuits before it can rate-limit us, which
	// is exactly the behaviour that makes retries cheap.
	if f.caps.NativeIdempotency && key != "" {
		if prev, ok := f.idem[key]; ok {
			return prev, nil
		}
	}
	if err := f.gate("upsert"); err != nil {
		return connector.WriteResult{}, err
	}

	// An account-level validation rule: permanently rejected, never retried.
	if f.f.RejectField != "" {
		if v, ok := rec.Fields[f.f.RejectField]; ok && fmt.Sprint(v) == f.f.RejectValue {
			return connector.WriteResult{}, connector.Errorf(f.name, "upsert",
				connector.KindPermanent, "property %q failed a validation rule", f.f.RejectField)
		}
	}

	created := false
	id := rec.RemoteID
	if id == "" {
		created = true
		f.nextID++
		id = fmt.Sprintf("%s-%d", f.name, f.nextID)
	}

	bucket, ok := f.data[kind]
	if !ok {
		bucket = make(map[string]*record)
		f.data[kind] = bucket
	}
	existing, ok := bucket[id]
	if !ok {
		created = true
		existing = &record{id: id, fields: make(map[string]any)}
		bucket[id] = existing
	}
	existing.deleted = false
	for k, v := range rec.Fields {
		existing.fields[k] = v
	}
	existing.updatedAt = f.stamp()
	existing.version++

	res := connector.WriteResult{
		RemoteID:  id,
		Created:   created,
		Version:   strconv.Itoa(existing.version),
		UpdatedAt: existing.updatedAt,
	}
	if f.caps.NativeIdempotency && key != "" {
		f.idem[key] = res
	}
	f.emit(model.ChangeEvent{Kind: kind, RemoteID: id, Record: ptr(f.toModel(kind, existing))})
	return res, nil
}

// Delete implements connector.Connector.
func (f *Fake) Delete(ctx context.Context, kind, remoteID, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.gate("delete"); err != nil {
		return err
	}
	rec, ok := f.data[kind][remoteID]
	if !ok {
		// Deleting an absent record is a success. A retry after a delete that
		// already landed must not fail, or every delete is a coin flip.
		return nil
	}
	if f.caps.SoftDelete {
		rec.deleted = true
		rec.updatedAt = f.stamp()
	} else {
		delete(f.data[kind], remoteID)
	}
	f.emit(model.ChangeEvent{Kind: kind, RemoteID: remoteID, Deleted: true})
	return nil
}

func (f *Fake) toModel(kind string, rec *record) model.Record {
	fields := make(map[string]any, len(rec.fields))
	for k, v := range rec.fields {
		fields[k] = v
	}
	out := model.Record{
		Kind:      kind,
		RemoteID:  rec.id,
		Fields:    fields,
		UpdatedAt: rec.updatedAt,
	}
	if f.caps.ETags {
		out.Version = strconv.Itoa(rec.version)
	}
	return out
}

func ptr(r model.Record) *model.Record { return &r }

// emit queues a webhook delivery. The caller holds the lock.
func (f *Fake) emit(e model.ChangeEvent) {
	if !f.caps.Webhooks {
		return
	}
	e.Source = f.name
	e.ObservedAt = f.now().UTC()
	if f.f.NotificationOnly {
		e.Record = nil
	}

	copies := 1
	if f.f.DuplicateWebhooks {
		copies = 2
	}
	f.seq++
	// A redelivery carries the same delivery ID, which is what makes it a
	// redelivery rather than a second event. Dedupe has to key on that.
	id := fmt.Sprintf("%s-d%d", f.name, f.seq)
	for i := 0; i < copies; i++ {
		f.queue = append(f.queue, pending{
			event:   e,
			dueAt:   f.now().Add(f.f.WebhookDelay),
			seq:     f.seq,
			delivID: id,
		})
	}
}
