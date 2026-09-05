package fake_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/connector"
	"crm-bisync/internal/connector/connectortest"
	"crm-bisync/internal/connector/fake"
	"crm-bisync/internal/model"
)

var epoch = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

func contactSchema() map[string]connector.Schema {
	return map[string]connector.Schema{
		"contact": {
			Kind: "contact",
			Fields: []connector.FieldSpec{
				{Name: "email", Type: "string", Required: true},
				{Name: "firstname", Type: "string"},
				{Name: "lastname", Type: "string"},
				{Name: "createdate", Type: "datetime", ReadOnly: true},
			},
		},
	}
}

func newFake(t *testing.T, caps connector.Caps, faults fake.Faults) (*fake.Fake, *clock.Manual) {
	t.Helper()
	c := clock.NewManual(epoch)
	return fake.New(fake.Options{
		Name:    "peer",
		Clock:   c,
		Caps:    caps,
		Secret:  "s3cr3t",
		Schemas: contactSchema(),
		Faults:  faults,
		Seed:    1,
	}), c
}

// The fake is held to the same bar the real connectors will be, in more than
// one capability shape, because the interesting bugs live in the branches the
// engine takes when a capability is missing.
func TestConformance(t *testing.T) {
	shapes := []struct {
		name  string
		caps  connector.Caps
		fault fake.Faults
	}{
		{
			name: "full-featured",
			caps: connector.Caps{
				NativeIdempotency: true, Webhooks: true, SoftDelete: true,
				ETags: true, ModifiedAtIsExact: true, MaxPageSize: 100,
			},
		},
		{
			name: "minimal",
			caps: connector.Caps{ModifiedAtIsExact: false},
			// A coarse clock and no idempotency: the shape that forces the
			// engine onto its fallback paths.
			fault: fake.Faults{TimestampGranularity: time.Second},
		},
		{
			name:  "paged",
			caps:  connector.Caps{Webhooks: true, MaxPageSize: 2},
			fault: fake.Faults{PageSize: 2},
		},
	}

	for _, shape := range shapes {
		shape := shape
		connectortest.Suite{
			Name:  shape.name,
			Kind:  "contact",
			Field: "email",
			New: func(t *testing.T) connector.Connector {
				f, _ := newFake(t, shape.caps, shape.fault)
				return f
			},
			SignDelivery: func(t *testing.T, c connector.Connector) *http.Request {
				t.Helper()
				f := c.(*fake.Fake)
				f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})
				deliveries := f.DrainWebhooks()
				if len(deliveries) == 0 {
					t.Fatal("no webhook was queued")
				}
				return deliveries[0].Request("/webhook/peer")
			},
		}.Run(t)
	}
}

func TestDuplicateDeliveriesShareADeliveryID(t *testing.T) {
	f, _ := newFake(t, connector.Caps{Webhooks: true}, fake.Faults{DuplicateWebhooks: true})
	f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})

	got := f.DrainWebhooks()
	if len(got) != 2 {
		t.Fatalf("got %d deliveries, want 2", len(got))
	}
	// This is the whole point: a redelivery is the same delivery, so dedupe
	// keys on the ID. Two different IDs would be two events, and dropping the
	// second would drop a real change.
	if got[0].ID != got[1].ID {
		t.Fatalf("redelivery has a different ID: %q and %q", got[0].ID, got[1].ID)
	}
}

func TestDelayedDeliveriesStayQueuedUntilDue(t *testing.T) {
	f, c := newFake(t, connector.Caps{Webhooks: true}, fake.Faults{WebhookDelay: 10 * time.Minute})
	f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})

	if got := f.DrainWebhooks(); len(got) != 0 {
		t.Fatalf("a delayed delivery came out early: %d", len(got))
	}
	if f.PendingWebhooks() != 1 {
		t.Fatalf("PendingWebhooks = %d, want 1", f.PendingWebhooks())
	}

	c.Advance(11 * time.Minute)
	if got := f.DrainWebhooks(); len(got) != 1 {
		t.Fatalf("delivery did not become due: %d", len(got))
	}
}

func TestNotificationOnlyDeliveriesCarryNoRecord(t *testing.T) {
	f, _ := newFake(t, connector.Caps{Webhooks: true}, fake.Faults{NotificationOnly: true})
	f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})

	d := f.DrainWebhooks()[0]
	events, err := f.VerifyWebhook(d.Request("/w"), d.Body)
	if err != nil {
		t.Fatalf("VerifyWebhook: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Hydrated() {
		t.Fatal("a notification-only delivery carried a record")
	}
	if events[0].RemoteID == "" {
		t.Fatal("a notification-only delivery must still say which record changed")
	}
}

func TestVerifyWebhookAttachesSourceAndDeliveryID(t *testing.T) {
	f, _ := newFake(t, connector.Caps{Webhooks: true}, fake.Faults{})
	f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})

	d := f.DrainWebhooks()[0]
	events, err := f.VerifyWebhook(d.Request("/w"), d.Body)
	if err != nil {
		t.Fatalf("VerifyWebhook: %v", err)
	}
	if events[0].Source != "peer" {
		t.Fatalf("Source = %q, want peer", events[0].Source)
	}
	if events[0].DeliveryID != d.ID {
		t.Fatalf("DeliveryID = %q, want %q", events[0].DeliveryID, d.ID)
	}
}

// A genuine signature over a stale timestamp is exactly what a captured
// delivery looks like when it is replayed, so the window is not optional.
func TestVerifyWebhookRejectsAStaleDelivery(t *testing.T) {
	f, c := newFake(t, connector.Caps{Webhooks: true}, fake.Faults{})
	f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})
	d := f.DrainWebhooks()[0]

	c.Advance(fake.SignatureWindow + time.Minute)

	_, err := f.VerifyWebhook(d.Request("/w"), d.Body)
	if err == nil {
		t.Fatal("a stale delivery was accepted")
	}
	if connector.KindOf(err) != connector.KindAuth {
		t.Fatalf("stale delivery gave %s, want auth", connector.KindOf(err))
	}
}

func TestVerifyWebhookRejectsAForeignSignature(t *testing.T) {
	f, _ := newFake(t, connector.Caps{Webhooks: true}, fake.Faults{})
	other := fake.New(fake.Options{
		Name: "other", Clock: clock.NewManual(epoch), Caps: connector.Caps{Webhooks: true},
		Secret: "a-different-secret", Schemas: contactSchema(),
	})

	body := fake.EncodeEvents(model.ChangeEvent{Kind: "contact", RemoteID: "x"})
	headers := other.SignBody(body, epoch)

	req, err := http.NewRequest(http.MethodPost, "/w", http.NoBody)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if _, err := f.VerifyWebhook(req, body); err == nil {
		t.Fatal("a delivery signed with another peer's secret was accepted")
	}
}

func TestVerifyWebhookRejectsMalformedBodyOnlyAfterAuth(t *testing.T) {
	f, _ := newFake(t, connector.Caps{Webhooks: true}, fake.Faults{})

	body := []byte("{not json")
	headers := f.SignBody(body, epoch)
	req, err := http.NewRequest(http.MethodPost, "/w", http.NoBody)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	_, err = f.VerifyWebhook(req, body)
	if err == nil {
		t.Fatal("malformed body was accepted")
	}
	// Authenticated but unparseable is the peer's bug, not an attack, so it
	// must not be reported as an auth failure or it will page the wrong person.
	if connector.KindOf(err) != connector.KindPermanent {
		t.Fatalf("malformed body gave %s, want permanent", connector.KindOf(err))
	}
}

func TestRateLimitIsClassifiedAndCarriesRetryAfter(t *testing.T) {
	f, _ := newFake(t, connector.Caps{}, fake.Faults{
		RateLimitEvery: 2, RateLimitRetryAfter: 7 * time.Second,
	})
	ctx := context.Background()

	if _, err := f.ListChanged(ctx, "contact", time.Time{}, ""); err != nil {
		t.Fatalf("first call: %v", err)
	}
	_, err := f.ListChanged(ctx, "contact", time.Time{}, "")
	if err == nil {
		t.Fatal("the second call was not rate limited")
	}
	if connector.KindOf(err) != connector.KindRateLimited {
		t.Fatalf("kind = %s, want rate_limited", connector.KindOf(err))
	}
	if got := connector.RetryAfterOf(err); got != 7*time.Second {
		t.Fatalf("RetryAfter = %s, want 7s", got)
	}
	if !connector.Retryable(err) {
		t.Fatal("a rate limit was classified as not retryable")
	}
}

func TestTransientAndPermanentAreDistinguished(t *testing.T) {
	ctx := context.Background()

	transient, _ := newFake(t, connector.Caps{}, fake.Faults{ServerErrorEvery: 1})
	_, err := transient.Get(ctx, "contact", "x")
	if connector.KindOf(err) != connector.KindTransient {
		t.Fatalf("kind = %s, want transient", connector.KindOf(err))
	}
	if !connector.Retryable(err) {
		t.Fatal("a 5xx was classified as not retryable")
	}

	// An account-level validation rule: the peer will never accept this write,
	// so retrying it just burns quota until the attempt budget runs out.
	permanent, _ := newFake(t, connector.Caps{}, fake.Faults{
		RejectField: "email", RejectValue: "blocked@example.com",
	})
	_, err = permanent.Upsert(ctx, "contact", model.Record{
		Kind: "contact", Fields: map[string]any{"email": "blocked@example.com"},
	}, "k")
	if connector.KindOf(err) != connector.KindPermanent {
		t.Fatalf("kind = %s, want permanent", connector.KindOf(err))
	}
	if connector.Retryable(err) {
		t.Fatal("a validation rejection was classified as retryable")
	}
}

// An unclassified error has to be retryable. Giving up on something we do not
// understand loses data; retrying it does not.
func TestUnclassifiedErrorsAreRetryable(t *testing.T) {
	if !connector.Retryable(context.DeadlineExceeded) {
		t.Fatal("an unclassified error was treated as permanent")
	}
	if connector.KindOf(nil) != connector.KindUnknown {
		t.Fatal("KindOf(nil) is not unknown")
	}
}

func TestCoarseTimestampsAreReportedCoarsely(t *testing.T) {
	f, c := newFake(t, connector.Caps{}, fake.Faults{TimestampGranularity: time.Minute})
	c.Advance(90 * time.Second)

	rec := f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})
	want := epoch.Add(time.Minute)
	if !rec.UpdatedAt.Equal(want) {
		t.Fatalf("UpdatedAt = %s, want %s (truncated to the minute)", rec.UpdatedAt, want)
	}
}

// The peer's clock is not ours. A watermark computed from our clock against
// their timestamps is exactly the bug the overlap window exists to absorb.
func TestClockOffsetSkewsReportedTimes(t *testing.T) {
	f, _ := newFake(t, connector.Caps{}, fake.Faults{ClockOffset: -90 * time.Second})
	rec := f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})

	want := epoch.Add(-90 * time.Second)
	if !rec.UpdatedAt.Equal(want) {
		t.Fatalf("UpdatedAt = %s, want %s", rec.UpdatedAt, want)
	}
}

func TestFieldRenameHitsSchemaAndRecords(t *testing.T) {
	faults := fake.Faults{}
	faults.RenameField.Kind = "contact"
	faults.RenameField.From = "firstname"
	faults.RenameField.To = "first_name"
	faults.RenameField.AfterCalls = 1

	f, _ := newFake(t, connector.Caps{}, faults)
	ctx := context.Background()
	res, err := f.Upsert(ctx, "contact", model.Record{
		Kind: "contact", Fields: map[string]any{"email": "a@b.c", "firstname": "Ann"},
	}, "k")
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	before, err := f.Describe(ctx, "contact")
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if _, ok := before.Field("firstname"); !ok {
		t.Fatal("the field was renamed too early")
	}

	after, err := f.Describe(ctx, "contact")
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if _, ok := after.Field("first_name"); !ok {
		t.Fatalf("the rename did not reach the schema: %v", after.Names())
	}
	if _, ok := after.Field("firstname"); ok {
		t.Fatal("the old field name is still in the schema")
	}
	if v, ok := f.Field("contact", res.RemoteID, "first_name"); !ok || v != "Ann" {
		t.Fatalf("the rename did not carry the value across: %v,%v", v, ok)
	}
}

// A person editing in the CRM's UI is not one of our API calls, so it must not
// consume the injected failures the engine's own requests are meant to hit.
func TestExternalEditsBypassTheFaultGate(t *testing.T) {
	f, _ := newFake(t, connector.Caps{}, fake.Faults{ServerErrorEvery: 1})

	f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})
	if f.Calls("upsert") != 0 {
		t.Fatalf("an external edit counted as %d API calls", f.Calls("upsert"))
	}
	if f.Count("contact") != 1 {
		t.Fatalf("Count = %d, want 1", f.Count("contact"))
	}
}

func TestSoftDeleteIsVisibleToPollingAndHardDeleteIsNot(t *testing.T) {
	ctx := context.Background()

	soft, _ := newFake(t, connector.Caps{SoftDelete: true}, fake.Faults{})
	rec := soft.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})
	soft.ExternalDelete("contact", rec.RemoteID)

	page, err := soft.ListChanged(ctx, "contact", time.Time{}, "")
	if err != nil {
		t.Fatalf("ListChanged: %v", err)
	}
	if len(page.Deleted) != 1 || page.Deleted[0] != rec.RemoteID {
		t.Fatalf("a soft-deleted record was not reported: %v", page.Deleted)
	}

	hard, _ := newFake(t, connector.Caps{}, fake.Faults{})
	rec2 := hard.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})
	hard.ExternalDelete("contact", rec2.RemoteID)

	page2, err := hard.ListChanged(ctx, "contact", time.Time{}, "")
	if err != nil {
		t.Fatalf("ListChanged: %v", err)
	}
	// Not a gap in the fake: on a peer that hard deletes, a removed record and
	// one that stopped being visible to our token look identical, and guessing
	// between them is how a sync deletes live data.
	if len(page2.Deleted) != 0 {
		t.Fatalf("a hard-deleting peer reported deletions: %v", page2.Deleted)
	}
}

func TestShuffleIsDeterministicForASeed(t *testing.T) {
	order := func() []string {
		f := fake.New(fake.Options{
			Name: "peer", Clock: clock.NewManual(epoch),
			Caps: connector.Caps{Webhooks: true}, Secret: "s",
			Schemas: contactSchema(),
			Faults:  fake.Faults{ShuffleWebhooks: true},
			Seed:    42,
		})
		for i := 0; i < 6; i++ {
			f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})
		}
		var ids []string
		for _, d := range f.DrainWebhooks() {
			ids = append(ids, d.ID)
		}
		return ids
	}

	a, b := order(), order()
	if len(a) != 6 {
		t.Fatalf("got %d deliveries, want 6", len(a))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("the same seed produced different orders: %v then %v", a, b)
		}
	}
}

func TestWebhookBodyIsValidJSONEvents(t *testing.T) {
	f, _ := newFake(t, connector.Caps{Webhooks: true}, fake.Faults{})
	f.ExternalUpsert("contact", "c-1", map[string]any{"email": "a@b.c"})

	d := f.DrainWebhooks()[0]
	var decoded struct {
		Events []model.ChangeEvent `json:"events"`
	}
	if err := json.Unmarshal(d.Body, &decoded); err != nil {
		t.Fatalf("delivery body is not valid JSON: %v", err)
	}
	if len(decoded.Events) != 1 || decoded.Events[0].RemoteID != "c-1" {
		t.Fatalf("unexpected body: %s", d.Body)
	}
}

func TestNoWebhooksWhenTheConnectorDoesNotSupportThem(t *testing.T) {
	f, _ := newFake(t, connector.Caps{Webhooks: false}, fake.Faults{})
	f.ExternalUpsert("contact", "", map[string]any{"email": "a@b.c"})

	if f.PendingWebhooks() != 0 {
		t.Fatal("a connector without webhook support queued a delivery")
	}
	if _, err := f.VerifyWebhook(&http.Request{Header: http.Header{}}, nil); err == nil {
		t.Fatal("VerifyWebhook succeeded on a connector without webhook support")
	}
}
