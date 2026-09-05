package engine_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"crm-bisync/internal/connector/fake"
)

// deliver posts one of a peer's queued webhooks at the engine.
func (h *harness) deliver(f *fake.Fake, path string) *httptest.ResponseRecorder {
	h.t.Helper()

	deliveries := f.DrainWebhooks()
	if len(deliveries) == 0 {
		h.t.Fatal("no delivery was queued")
	}
	return h.post(deliveries[0].Request(path))
}

func (h *harness) post(req *http.Request) *httptest.ResponseRecorder {
	h.t.Helper()
	rec := httptest.NewRecorder()
	h.engine.WebhookHandler().ServeHTTP(rec, req)
	return rec
}

func TestWebhookAcceptsASignedDeliveryAndQueuesIt(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})

	res := h.deliver(h.left, "/webhook/left")
	if res.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", res.Code)
	}
	if h.engine.QueueDepth() == 0 {
		t.Fatal("the delivery was accepted but nothing was queued")
	}

	h.engine.Drain(context.Background())
	if got := h.right.Count("contact"); got != 1 {
		t.Fatalf("the queued change did not sync: right has %d records", got)
	}
}

// A redelivery carries the same identifier as the original, which is what
// makes it a redelivery rather than a second event.
func TestWebhookRedeliveriesAreDeduplicated(t *testing.T) {
	h := newHarness(t, harnessOptions{leftFaults: fake.Faults{DuplicateWebhooks: true}})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})

	deliveries := h.left.DrainWebhooks()
	if len(deliveries) != 2 {
		t.Fatalf("got %d deliveries, want 2", len(deliveries))
	}
	for _, d := range deliveries {
		if res := h.post(d.Request("/webhook/left")); res.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", res.Code)
		}
	}

	if got := h.engine.Stats().Ingested; got != 1 {
		t.Fatalf("%d events were queued for one change", got)
	}
}

func TestWebhookRejectsABadSignature(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})

	d := h.left.DrainWebhooks()[0]
	req := d.Request("/webhook/left")
	req.Header.Set(fake.HeaderSignature, "0000")

	res := h.post(req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", res.Code)
	}
	if h.engine.QueueDepth() != 0 {
		t.Fatal("a delivery that failed verification was queued anyway")
	}
}

// A genuine signature over a stale timestamp is what a captured delivery looks
// like when it is replayed.
func TestWebhookRejectsAStaleDelivery(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	d := h.left.DrainWebhooks()[0]

	h.clock.Advance(fake.SignatureWindow + time.Minute)

	if res := h.post(d.Request("/webhook/left")); res.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", res.Code)
	}
}

func TestWebhookRoutingAndMethod(t *testing.T) {
	h := newHarness(t, harnessOptions{})

	if res := h.post(httptest.NewRequest(http.MethodGet, "/webhook/left", nil)); res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", res.Code)
	}
	if res := h.post(httptest.NewRequest(http.MethodPost, "/webhook/pipedrive", nil)); res.Code != http.StatusNotFound {
		t.Fatalf("unknown connector status = %d, want 404", res.Code)
	}
}

// A delivery signed by one peer must not be accepted as the other's, or a
// connector's secret would protect nothing but itself.
func TestWebhookRejectsAnotherPeersDelivery(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})
	d := h.left.DrainWebhooks()[0]

	if res := h.post(d.Request("/webhook/right")); res.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", res.Code)
	}
}

// The write we make causes a webhook. Processing it must not cause another.
func TestOurOwnWriteComesBackAndStopsThere(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})

	ctx := context.Background()
	h.deliver(h.left, "/webhook/left")
	h.engine.Drain(ctx)

	writes := h.writes()
	if h.right.PendingWebhooks() == 0 {
		t.Fatal("the write did not cause a delivery, so there is nothing to test")
	}

	// Everything the write caused, fed back in.
	for round := 0; round < 5; round++ {
		for _, d := range h.right.DrainWebhooks() {
			h.post(d.Request("/webhook/right"))
		}
		for _, d := range h.left.DrainWebhooks() {
			h.post(d.Request("/webhook/left"))
		}
		h.engine.Drain(ctx)
	}

	if got := h.writes(); got != writes {
		t.Fatalf("%d further writes came out of feeding back our own echoes (%d then %d)",
			got-writes, writes, got)
	}
	if h.engine.Stats().Echoes == 0 {
		t.Fatal("nothing was recognised as an echo")
	}
	if h.left.Count("contact") != 1 || h.right.Count("contact") != 1 {
		t.Fatalf("record counts drifted: left %d, right %d",
			h.left.Count("contact"), h.right.Count("contact"))
	}
}

// A peer that only says "record X changed" makes the engine fetch, which is
// the price of a peer that tells us less rather than a design choice.
func TestNotificationOnlyDeliveriesAreHydrated(t *testing.T) {
	h := newHarness(t, harnessOptions{leftFaults: fake.Faults{NotificationOnly: true}})
	h.left.ExternalUpsert("contact", "", map[string]any{"email": "ann@example.com", "firstname": "Ann"})

	h.deliver(h.left, "/webhook/left")
	h.engine.Drain(context.Background())

	if got := h.right.Count("contact"); got != 1 {
		t.Fatalf("a notification-only delivery did not sync: right has %d records", got)
	}
	if h.left.Calls("get") == 0 {
		t.Fatal("the engine did not fetch the record it was only told about")
	}
}
