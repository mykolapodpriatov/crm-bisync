// Package engine wires the pieces into a sync.
//
// Every other package in this repository is a mechanism tested in isolation:
// echo suppression, identity resolution, idempotency, watermarks, rate
// limiting, conflict resolution. This is the one that makes them a system, and
// almost all of its content is ordering. Which stage runs before which, what
// is written before the network call rather than after it, and what has to
// hold a lock, are the decisions that decide whether the mechanisms compose or
// merely coexist.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/config"
	"crm-bisync/internal/conflict"
	"crm-bisync/internal/connector"
	"crm-bisync/internal/idem"
	"crm-bisync/internal/identity"
	"crm-bisync/internal/mapping"
	"crm-bisync/internal/origin"
	"crm-bisync/internal/ratelimit"
	"crm-bisync/internal/store"
	"crm-bisync/internal/watermark"
)

// Peer is one side of one sync: a connector, the side of the mapping it sits
// on, and the bucket guarding its quota.
type Peer struct {
	Name   string
	Conn   connector.Connector
	Side   mapping.Side
	Bucket *ratelimit.Bucket
	Caps   connector.Caps
}

// Sync is one configured object type flowing between two peers.
type Sync struct {
	Name  string
	Kind  string
	Left  *Peer
	Right *Peer

	Mapper   *mapping.Mapper
	Identity *identity.Resolver
	Conflict *conflict.Resolver

	// canonical is the field set the digest is computed over, cached because
	// it is needed on every event.
	canonical []string
}

// peerByName returns the peer with a connector name.
func (s *Sync) peerByName(name string) (*Peer, bool) {
	switch name {
	case s.Left.Name:
		return s.Left, true
	case s.Right.Name:
		return s.Right, true
	default:
		return nil, false
	}
}

// other returns the peer opposite the given one.
func (s *Sync) other(p *Peer) *Peer {
	if p == s.Left {
		return s.Right
	}
	return s.Left
}

// Options configures an engine.
type Options struct {
	Config *config.Config
	Store  store.Store
	Clock  clock.Clock
	Logger *slog.Logger
	// Connectors are the live connectors, keyed by the name used in config.
	Connectors map[string]connector.Connector
	// Seed makes retry jitter reproducible. Tests set it; production leaves
	// it zero and gets the clock.
	Seed int64
	// DryRun records writes instead of sending them. Callers normally reach
	// this through Plan, which also arranges for the state the run produces
	// to be thrown away.
	DryRun bool
}

// Engine runs the syncs.
type Engine struct {
	cfg    *config.Config
	clock  clock.Clock
	store  store.Store
	typed  *store.Typed
	log    *slog.Logger
	syncs  []*Sync
	byName map[string]*Sync

	origin  *origin.Suppressor
	idem    *idem.Cache
	marks   *watermark.Tracker
	buckets *ratelimit.Set

	queue *workQueue
	locks *keyedMutex

	// plan is non-nil in a dry run, and is what the writes go into instead of
	// the network.
	plan *planRecorder

	rndMu sync.Mutex
	rnd   *rand.Rand

	statsMu sync.Mutex
	stats   Stats

	startOnce sync.Once
	stopOnce  sync.Once
	workersWG sync.WaitGroup
	stopped   chan struct{}
}

// New builds an engine from configuration and a set of live connectors.
//
// Everything that can be checked without the network is checked here rather
// than at the first event: a sync that names a connector nobody supplied, or a
// mapping that cannot be built, should stop the process at boot with the name
// in the message.
func New(opts Options) (*Engine, error) {
	if opts.Config == nil {
		return nil, errors.New("engine: no configuration")
	}
	if opts.Store == nil {
		return nil, errors.New("engine: no store")
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	seed := opts.Seed
	if seed == 0 {
		seed = opts.Clock.Now().UnixNano()
	}

	e := &Engine{
		cfg:     opts.Config,
		clock:   opts.Clock,
		store:   opts.Store,
		typed:   store.NewTyped(opts.Store),
		log:     opts.Logger,
		byName:  map[string]*Sync{},
		buckets: ratelimit.NewSet(opts.Clock),
		locks:   newKeyedMutex(),
		rnd:     rand.New(rand.NewSource(seed)),
		stopped: make(chan struct{}),
	}
	if opts.DryRun {
		e.plan = &planRecorder{}
	}

	windows := map[string]time.Duration{}
	peers := map[string]watermark.PeerConfig{}
	for _, c := range opts.Config.Connectors {
		conn, ok := opts.Connectors[c.Name]
		if !ok {
			return nil, fmt.Errorf("engine: no connector supplied for %q", c.Name)
		}
		e.buckets.Add(c.Name, c.RateLimit.RatePerSec, c.RateLimit.Burst)
		windows[c.Name] = c.EchoWindow.D()
		peers[c.Name] = watermark.PeerConfig{
			Overlap:         c.Overlap.D(),
			ExactTimestamps: conn.Capabilities().ModifiedAtIsExact,
		}
	}

	e.origin = origin.New(e.typed, opts.Clock, windows)
	e.idem = idem.NewCache(e.typed, opts.Clock, 0)
	e.marks = watermark.New(e.typed, opts.Clock, peers)

	for i, cs := range opts.Config.Syncs {
		s, err := e.buildSync(cs, opts.Connectors)
		if err != nil {
			return nil, fmt.Errorf("engine: syncs[%d]: %w", i, err)
		}
		e.syncs = append(e.syncs, s)
		e.byName[s.Name] = s
	}

	e.queue = newWorkQueue(opts.Config.Workers)
	return e, nil
}

// buildSync turns one configured sync into a runnable one.
func (e *Engine) buildSync(cs config.Sync, conns map[string]connector.Connector) (*Sync, error) {
	mapper, err := mapping.New(cs.MappingSpec())
	if err != nil {
		return nil, err
	}

	resolver, err := conflict.New(cs.Conflict.Default, cs.Conflict.PerField, 0)
	if err != nil {
		return nil, err
	}

	peer := func(name string, side mapping.Side) (*Peer, error) {
		conn, ok := conns[name]
		if !ok {
			return nil, fmt.Errorf("no connector supplied for %q", name)
		}
		return &Peer{
			Name:   name,
			Conn:   conn,
			Side:   side,
			Bucket: e.buckets.For(name),
			Caps:   conn.Capabilities(),
		}, nil
	}

	left, err := peer(cs.Left, mapping.Left)
	if err != nil {
		return nil, err
	}
	right, err := peer(cs.Right, mapping.Right)
	if err != nil {
		return nil, err
	}

	return &Sync{
		Name:      fmt.Sprintf("%s:%s-%s", cs.Kind, cs.Left, cs.Right),
		Kind:      cs.Kind,
		Left:      left,
		Right:     right,
		Mapper:    mapper,
		Conflict:  resolver,
		Identity:  identity.New(e.typed, cs.Kind, cs.Identity.Keys, cs.Identity.OnAmbiguous),
		canonical: mapper.CanonicalNames(),
	}, nil
}

// Syncs returns the configured syncs, for the CLI and for tests.
func (e *Engine) Syncs() []*Sync { return e.syncs }

// Validate checks every mapping against both peers' live schemas.
//
// This is what doctor runs, and what Start runs before accepting any work: a
// renamed property should fail at boot with the field name in the message,
// not at three in the morning as a write that half-lands.
func (e *Engine) Validate(ctx context.Context) error {
	var errs []error
	for _, s := range e.syncs {
		left, err := s.Left.Conn.Describe(ctx, s.Kind)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: describing %s: %w", s.Name, s.Left.Name, err))
			continue
		}
		right, err := s.Right.Conn.Describe(ctx, s.Kind)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: describing %s: %w", s.Name, s.Right.Name, err))
			continue
		}
		if err := s.Mapper.Validate(left, right); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", s.Name, err))
		}
	}
	return errors.Join(errs...)
}
