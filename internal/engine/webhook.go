package engine

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"crm-bisync/internal/connector"
	"crm-bisync/internal/model"
)

// maxWebhookBody caps what an unauthenticated caller can make us read into
// memory. The endpoint has to be reachable by the peer, which means it is
// reachable by anyone, and a body limit is the cheapest part of accepting that.
const maxWebhookBody = 4 << 20

// deliveryTTL is how long a delivery ID is remembered. It only has to outlive
// the peer's redelivery window.
const deliveryTTL = 24 * time.Hour

// WebhookHandler serves inbound deliveries at /webhook/{connector}.
//
// The route is necessarily unauthenticated in the Drupal sense: a CRM cannot
// hold a session. What protects it is the connector's own signature check,
// which is why nothing here looks at the body before VerifyWebhook has run.
func (e *Engine) WebhookHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook/", e.serveWebhook)
	return mux
}

func (e *Engine) serveWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/webhook/")
	if name == "" || strings.Contains(name, "/") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	peers := e.peersNamed(name)
	if len(peers) == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody))
	if err != nil {
		http.Error(w, "could not read body", http.StatusBadRequest)
		return
	}

	// Any peer with this connector name shares the connector instance, so one
	// verification answers for all of them.
	events, err := peers[0].Conn.VerifyWebhook(r, body)
	if err != nil {
		status := http.StatusBadRequest
		if connector.KindOf(err) == connector.KindAuth {
			status = http.StatusUnauthorized
		}
		e.log.Warn("rejected a delivery", "connector", name, "err", err)
		http.Error(w, http.StatusText(status), status)
		return
	}

	accepted, err := e.ingest(name, events)
	if err != nil {
		e.log.Error("could not record a delivery", "connector", name, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Accepted, not processed. Doing the work inside the request would make
	// the peer's delivery timeout our retry policy, and it has a worse one.
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(http.StatusText(http.StatusAccepted)))
	e.log.Debug("accepted a delivery", "connector", name, "events", accepted)
}

// ingest deduplicates a delivery and queues its events.
func (e *Engine) ingest(connectorName string, events []model.ChangeEvent) (int, error) {
	queued := 0
	var errs []error

	for _, ev := range events {
		ev.Source = connectorName

		if ev.DeliveryID != "" {
			// A redelivery carries the same identifier as the original, which
			// is what makes it a redelivery rather than a second event. The
			// dedupe is per event within the delivery, so a delivery carrying
			// several changes is not collapsed to one.
			key := ev.DeliveryID + "\x00" + ev.Kind + "\x00" + ev.RemoteID
			seen, err := e.typed.SeenDelivery(connectorName, key,
				e.clock.Now(), e.clock.Now().Add(deliveryTTL))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if seen {
				continue
			}
		}

		for _, s := range e.syncsFor(connectorName, ev.Kind) {
			e.Submit(s.Name, ev)
			queued++
		}
	}
	return queued, errors.Join(errs...)
}

// peersNamed returns every peer using a connector name.
func (e *Engine) peersNamed(name string) []*Peer {
	var out []*Peer
	for _, s := range e.syncs {
		if p, ok := s.peerByName(name); ok {
			out = append(out, p)
		}
	}
	return out
}

// syncsFor returns the syncs a change on this connector and kind belongs to.
func (e *Engine) syncsFor(connectorName, kind string) []*Sync {
	var out []*Sync
	for _, s := range e.syncs {
		if s.Kind != kind {
			continue
		}
		if _, ok := s.peerByName(connectorName); ok {
			out = append(out, s)
		}
	}
	return out
}
