// Package connectortest holds the conformance suite every connector must pass.
//
// It exists so that "the fake behaves like a CRM" and "the HubSpot adapter
// behaves like a CRM" are the same claim, checked the same way. The fake runs
// it in unit tests; the real adapters run it behind a build tag against a
// developer account or a local container.
//
// It is a normal package rather than a _test package because connectors live
// in their own packages and each needs to import it.
package connectortest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"crm-bisync/internal/connector"
	"crm-bisync/internal/model"
)

// Suite describes how to exercise one connector.
type Suite struct {
	// Name labels the subtests.
	Name string
	// Kind is an object type the connector supports.
	Kind string
	// New returns a connector ready to use. It is called once per subtest, so
	// tests cannot leak state into each other.
	New func(t *testing.T) connector.Connector
	// Field is a writable string field on Kind that the suite may set freely.
	Field string
	// Value builds a distinct value for Field. Real connectors need this to
	// avoid colliding with data already in a shared test account.
	Value func(seq int) string
	// SignDelivery renders a valid signed webhook request, or nil when the
	// connector does not accept webhooks. It lets the suite check that a
	// tampered body is rejected without knowing the signing scheme.
	SignDelivery func(t *testing.T, c connector.Connector) *http.Request
}

// Run executes the suite.
func (s Suite) Run(t *testing.T) {
	t.Helper()
	if s.Value == nil {
		s.Value = func(seq int) string { return fmt.Sprintf("conformance-%d", seq) }
	}

	t.Run(s.Name+"/Describe", s.testDescribe)
	t.Run(s.Name+"/UpsertCreatesAndGetReads", s.testUpsertCreatesAndGetReads)
	t.Run(s.Name+"/UpsertWithIDUpdates", s.testUpsertWithIDUpdates)
	t.Run(s.Name+"/ListChangedSeesTheWrite", s.testListChangedSeesTheWrite)
	t.Run(s.Name+"/ListChangedPagesCoverEverythingOnce", s.testPagingCoversEverythingOnce)
	t.Run(s.Name+"/GetOfAbsentRecordIsNotFound", s.testGetAbsentIsNotFound)
	t.Run(s.Name+"/DeleteRemovesAndIsRepeatable", s.testDeleteIsRepeatable)
	t.Run(s.Name+"/IdempotencyKeyIsHonoured", s.testIdempotency)
	t.Run(s.Name+"/WebhookRejectsATamperedBody", s.testWebhookRejectsTamperedBody)
	t.Run(s.Name+"/ContextCancellationIsRespected", s.testContextCancellation)
}

func (s Suite) testDescribe(t *testing.T) {
	c := s.New(t)
	schema, err := c.Describe(context.Background(), s.Kind)
	if err != nil {
		t.Fatalf("Describe(%s): %v", s.Kind, err)
	}
	if schema.Kind != s.Kind {
		t.Fatalf("Describe reported kind %q, want %q", schema.Kind, s.Kind)
	}
	if _, ok := schema.Field(s.Field); !ok {
		t.Fatalf("schema has no field %q; it has %v", s.Field, schema.Names())
	}

	// A kind the connector does not have must be a classified not-found, not
	// an empty schema, or mapping validation would pass against nothing.
	if _, err := c.Describe(context.Background(), "definitely-not-an-object"); err == nil {
		t.Fatal("Describe of an unknown kind succeeded")
	} else if !connector.IsNotFound(err) {
		t.Fatalf("Describe of an unknown kind gave %s, want not_found", connector.KindOf(err))
	}
}

func (s Suite) testUpsertCreatesAndGetReads(t *testing.T) {
	ctx := context.Background()
	c := s.New(t)
	want := s.Value(1)

	res, err := c.Upsert(ctx, s.Kind, model.Record{
		Kind:   s.Kind,
		Fields: map[string]any{s.Field: want},
	}, "conformance-create-1")
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if res.RemoteID == "" {
		t.Fatal("Upsert returned an empty remote ID")
	}
	if !res.Created {
		t.Fatal("Upsert of a new record reported Created = false")
	}

	got, err := c.Get(ctx, s.Kind, res.RemoteID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.RemoteID != res.RemoteID {
		t.Fatalf("Get returned %q, want %q", got.RemoteID, res.RemoteID)
	}
	if fmt.Sprint(got.Fields[s.Field]) != want {
		t.Fatalf("field did not round-trip: %v, want %q", got.Fields[s.Field], want)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("Get returned a record with no modification time; watermarks need one")
	}
	if c.Capabilities().ETags && got.Version == "" {
		t.Fatal("connector claims ETags but returned no version")
	}
}

func (s Suite) testUpsertWithIDUpdates(t *testing.T) {
	ctx := context.Background()
	c := s.New(t)

	first, err := c.Upsert(ctx, s.Kind, model.Record{
		Kind: s.Kind, Fields: map[string]any{s.Field: s.Value(1)},
	}, "conformance-update-1")
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	second, err := c.Upsert(ctx, s.Kind, model.Record{
		Kind: s.Kind, RemoteID: first.RemoteID,
		Fields: map[string]any{s.Field: s.Value(2)},
	}, "conformance-update-2")
	if err != nil {
		t.Fatalf("Upsert with an ID: %v", err)
	}
	if second.Created {
		t.Fatal("Upsert with an existing ID reported Created = true")
	}
	if second.RemoteID != first.RemoteID {
		t.Fatalf("update moved the record from %q to %q", first.RemoteID, second.RemoteID)
	}

	got, err := c.Get(ctx, s.Kind, first.RemoteID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if fmt.Sprint(got.Fields[s.Field]) != s.Value(2) {
		t.Fatalf("update did not stick: %v", got.Fields[s.Field])
	}
}

func (s Suite) testListChangedSeesTheWrite(t *testing.T) {
	ctx := context.Background()
	c := s.New(t)

	res, err := c.Upsert(ctx, s.Kind, model.Record{
		Kind: s.Kind, Fields: map[string]any{s.Field: s.Value(1)},
	}, "conformance-list-1")
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	page, err := c.ListChanged(ctx, s.Kind, time.Time{}, "")
	if err != nil {
		t.Fatalf("ListChanged: %v", err)
	}
	if !containsID(page.Records, res.RemoteID) {
		t.Fatalf("a record written a moment ago is not in the delta read; got %d records",
			len(page.Records))
	}

	// A watermark in the future must return nothing, or every poll is a full
	// scan and the watermark is decorative.
	future, err := c.ListChanged(ctx, s.Kind, time.Now().Add(24*time.Hour), "")
	if err != nil {
		t.Fatalf("ListChanged(future): %v", err)
	}
	if len(future.Records) != 0 {
		t.Fatalf("a watermark 24h in the future still returned %d records", len(future.Records))
	}
}

// Paging must cover every record exactly once. A connector that repeats a
// record across pages makes the engine do redundant writes; one that drops a
// record loses data silently, which is worse.
func (s Suite) testPagingCoversEverythingOnce(t *testing.T) {
	ctx := context.Background()
	c := s.New(t)

	const n = 7
	want := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		res, err := c.Upsert(ctx, s.Kind, model.Record{
			Kind: s.Kind, Fields: map[string]any{s.Field: s.Value(i)},
		}, fmt.Sprintf("conformance-page-%d", i))
		if err != nil {
			t.Fatalf("Upsert %d: %v", i, err)
		}
		want[res.RemoteID] = true
	}

	seen := make(map[string]int)
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > n+5 {
			t.Fatal("paging did not terminate")
		}
		page, err := c.ListChanged(ctx, s.Kind, time.Time{}, cursor)
		if err != nil {
			t.Fatalf("ListChanged(page %d): %v", pages, err)
		}
		for _, rec := range page.Records {
			seen[rec.RemoteID]++
		}
		if page.Cursor == "" {
			break
		}
		cursor = page.Cursor
	}

	for id := range want {
		switch seen[id] {
		case 1:
		case 0:
			t.Fatalf("record %q was never returned by paging", id)
		default:
			t.Fatalf("record %q was returned %d times across pages", id, seen[id])
		}
	}
}

func (s Suite) testGetAbsentIsNotFound(t *testing.T) {
	c := s.New(t)
	_, err := c.Get(context.Background(), s.Kind, "definitely-not-a-real-id")
	if err == nil {
		t.Fatal("Get of an absent record succeeded")
	}
	if !connector.IsNotFound(err) {
		t.Fatalf("Get of an absent record gave %s, want not_found", connector.KindOf(err))
	}
}

// Deleting twice has to succeed. A retry after a delete that already landed is
// the normal case, not an exception, so a connector that errors on the second
// call turns every delete into a coin flip.
func (s Suite) testDeleteIsRepeatable(t *testing.T) {
	ctx := context.Background()
	c := s.New(t)

	res, err := c.Upsert(ctx, s.Kind, model.Record{
		Kind: s.Kind, Fields: map[string]any{s.Field: s.Value(1)},
	}, "conformance-delete-1")
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := c.Delete(ctx, s.Kind, res.RemoteID, "conformance-delete-key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get(ctx, s.Kind, res.RemoteID); !connector.IsNotFound(err) {
		t.Fatalf("record is still readable after Delete (err = %v)", err)
	}
	if err := c.Delete(ctx, s.Kind, res.RemoteID, "conformance-delete-key"); err != nil {
		t.Fatalf("deleting an already-deleted record failed: %v", err)
	}
}

func (s Suite) testIdempotency(t *testing.T) {
	ctx := context.Background()
	c := s.New(t)
	if !c.Capabilities().NativeIdempotency {
		t.Skipf("%s does not claim native idempotency; the engine supplies the fallback", s.Name)
	}

	rec := model.Record{Kind: s.Kind, Fields: map[string]any{s.Field: s.Value(1)}}
	const key = "conformance-idem-1"

	first, err := c.Upsert(ctx, s.Kind, rec, key)
	if err != nil {
		t.Fatalf("first Upsert: %v", err)
	}
	second, err := c.Upsert(ctx, s.Kind, rec, key)
	if err != nil {
		t.Fatalf("replayed Upsert: %v", err)
	}
	if second.RemoteID != first.RemoteID {
		t.Fatalf("replaying an idempotency key created a second record: %q then %q",
			first.RemoteID, second.RemoteID)
	}
	if !second.Created {
		t.Fatal("a replayed key reported Created = false; it must report the original outcome")
	}
}

func (s Suite) testWebhookRejectsTamperedBody(t *testing.T) {
	c := s.New(t)
	if !c.Capabilities().Webhooks || s.SignDelivery == nil {
		t.Skipf("%s does not accept webhooks", s.Name)
	}

	req := s.SignDelivery(t, c)
	body := readBody(t, req)

	if _, err := c.VerifyWebhook(req, body); err != nil {
		t.Fatalf("a correctly signed delivery was rejected: %v", err)
	}

	tampered := append([]byte(nil), body...)
	tampered = append(tampered, ' ')
	if _, err := c.VerifyWebhook(req, tampered); err == nil {
		t.Fatal("a tampered body passed verification")
	} else if connector.KindOf(err) != connector.KindAuth {
		t.Fatalf("tampered body gave %s, want auth", connector.KindOf(err))
	}

	stripped := s.SignDelivery(t, c)
	stripped.Header.Del("X-Fake-Signature")
	stripped.Header.Set("Signature", "")
	if _, err := c.VerifyWebhook(stripped, readBody(t, stripped)); err == nil {
		t.Fatal("a delivery with no signature passed verification")
	}
}

// A cancelled context must stop work. The engine cancels on shutdown and when
// a client disconnects, and a connector that ignores it leaks a goroutine per
// request.
func (s Suite) testContextCancellation(t *testing.T) {
	c := s.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.ListChanged(ctx, s.Kind, time.Time{}, "")
	if err == nil {
		t.Fatal("ListChanged ignored a cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ListChanged returned %v, want context.Canceled", err)
	}
}

func containsID(records []model.Record, id string) bool {
	for _, r := range records {
		if r.RemoteID == id {
			return true
		}
	}
	return false
}

func readBody(t *testing.T, r *http.Request) []byte {
	t.Helper()
	if r.Body == nil {
		return nil
	}
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 512)
	for {
		n, err := r.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
	}
	return buf
}
