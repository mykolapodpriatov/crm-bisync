package fake

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"crm-bisync/internal/connector"
	"crm-bisync/internal/model"
)

// payload is the wire shape of a delivery.
type payload struct {
	Events []model.ChangeEvent `json:"events"`
}

// sign computes the signature over the timestamp and the body.
//
// The timestamp is inside the signed material on purpose. Signing the body
// alone produces a signature that stays valid forever, so anyone who captures
// one delivery can replay it indefinitely.
func sign(secret []byte, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d.", ts)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyWebhook implements connector.Connector.
//
// Order matters here and the code follows it deliberately: authenticate the
// request, check it is not a replay, and only then look at the body. Parsing
// first would mean acting on attacker-controlled JSON before knowing whether
// it came from the peer at all.
func (f *Fake) VerifyWebhook(r *http.Request, body []byte) ([]model.ChangeEvent, error) {
	if !f.caps.Webhooks {
		return nil, connector.Errorf(f.name, "webhook", connector.KindPermanent,
			"this connector does not accept webhooks")
	}

	rawTS := r.Header.Get(HeaderTimestamp)
	if rawTS == "" {
		return nil, connector.Errorf(f.name, "webhook", connector.KindAuth, "missing timestamp")
	}
	ts, err := strconv.ParseInt(rawTS, 10, 64)
	if err != nil {
		return nil, connector.Errorf(f.name, "webhook", connector.KindAuth, "bad timestamp %q", rawTS)
	}

	got := r.Header.Get(HeaderSignature)
	want := sign(f.sec, ts, body)
	// hmac.Equal, never ==: a byte-by-byte comparison leaks how much of the
	// signature was correct through its timing.
	if !hmac.Equal([]byte(got), []byte(want)) {
		return nil, connector.Errorf(f.name, "webhook", connector.KindAuth, "bad signature")
	}

	age := f.now().Sub(time.Unix(ts, 0))
	if age < 0 {
		age = -age
	}
	if age > SignatureWindow {
		return nil, connector.Errorf(f.name, "webhook", connector.KindAuth,
			"delivery is %s old, window is %s", age, SignatureWindow)
	}

	var p payload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, connector.Errorf(f.name, "webhook", connector.KindPermanent,
			"malformed body: %v", err)
	}

	delivery := r.Header.Get(HeaderDelivery)
	out := make([]model.ChangeEvent, 0, len(p.Events))
	for _, e := range p.Events {
		e.Source = f.name
		e.DeliveryID = delivery
		out = append(out, e)
	}
	return out, nil
}

// Delivery is one signed webhook the fake wants to send.
type Delivery struct {
	ID      string
	Body    []byte
	Headers map[string]string
}

// Request renders the delivery as an HTTP request aimed at path.
func (d Delivery) Request(path string) *http.Request {
	r, err := http.NewRequest(http.MethodPost, path, bytes.NewReader(d.Body))
	if err != nil {
		// The inputs are ours and constant, so this cannot fail in practice.
		panic("fake: build webhook request: " + err.Error())
	}
	for k, v := range d.Headers {
		r.Header.Set(k, v)
	}
	return r
}

// DrainWebhooks returns every delivery that is due, signed and ready to post,
// and removes it from the queue.
//
// Deliveries that are not yet due stay queued: that is how Faults.WebhookDelay
// lets a test arrange for our own write to come back after the engine's echo
// window has already closed, which is the case that turns a working sync into
// an infinite one.
func (f *Fake) DrainWebhooks() []Delivery {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := f.now()
	var due []pending
	kept := f.queue[:0]
	for _, p := range f.queue {
		if p.dueAt.After(now) {
			kept = append(kept, p)
			continue
		}
		due = append(due, p)
	}
	f.queue = kept

	if f.f.ShuffleWebhooks && len(due) > 1 {
		f.rnd.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	}

	out := make([]Delivery, 0, len(due))
	for _, p := range due {
		body, err := json.Marshal(payload{Events: []model.ChangeEvent{p.event}})
		if err != nil {
			panic("fake: encode webhook: " + err.Error())
		}
		ts := now.Unix()
		out = append(out, Delivery{
			ID:   p.delivID,
			Body: body,
			Headers: map[string]string{
				HeaderTimestamp: strconv.FormatInt(ts, 10),
				HeaderSignature: sign(f.sec, ts, body),
				HeaderDelivery:  p.delivID,
			},
		})
	}
	return out
}

// PendingWebhooks reports how many deliveries are queued, due or not.
func (f *Fake) PendingWebhooks() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queue)
}

// SignBody signs an arbitrary body as this peer would, for tests that need to
// forge a delivery rather than take one off the queue.
func (f *Fake) SignBody(body []byte, at time.Time) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ts := at.Unix()
	return map[string]string{
		HeaderTimestamp: strconv.FormatInt(ts, 10),
		HeaderSignature: sign(f.sec, ts, body),
		HeaderDelivery:  "forged-" + strconv.FormatInt(ts, 10),
	}
}

// EncodeEvents renders events into a delivery body.
func EncodeEvents(events ...model.ChangeEvent) []byte {
	body, err := json.Marshal(payload{Events: events})
	if err != nil {
		panic("fake: encode events: " + err.Error())
	}
	return body
}
