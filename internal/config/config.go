// Package config loads and validates the crm-bisync configuration.
//
// Validation is deliberately strict and its errors name the offending path,
// because a misconfigured sync does not fail loudly at runtime: it quietly
// writes the wrong thing into somebody's CRM. Every problem the loader can
// detect without touching the network is detected here, at startup.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Duration is a time.Duration that reads as a Go duration string in JSON, so
// the config says "5m" rather than 300000000000.
type Duration time.Duration

// UnmarshalJSON parses a duration string such as "5m" or "1h30m".
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string such as \"5m\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalJSON writes the duration back as a string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Config is the whole file.
type Config struct {
	LogLevel    string      `json:"log_level"`
	Listen      string      `json:"listen"`
	StateDir    string      `json:"state_dir"`
	Workers     int         `json:"workers"`
	MaxAttempts int         `json:"max_attempts"`
	Connectors  []Connector `json:"connectors"`
	Syncs       []Sync      `json:"syncs"`
}

// Connector is one side of a sync.
type Connector struct {
	Name   string `json:"name"`
	Driver string `json:"driver"`
	// EchoWindow is how long an outbound write stays in the origin log. It has
	// to comfortably exceed the peer's webhook delivery latency, otherwise our
	// own write comes back after the entry expired and starts a loop.
	EchoWindow Duration `json:"echo_window"`
	// WatermarkOverlap is how far back each delta read reaches beyond the
	// stored watermark, to absorb peer clock skew and coarse timestamps.
	Overlap   Duration          `json:"watermark_overlap"`
	RateLimit RateLimit         `json:"rate_limit"`
	Options   map[string]string `json:"options,omitempty"`
}

// RateLimit is a token bucket sized for one connector's account-wide quota.
type RateLimit struct {
	RatePerSec float64 `json:"rate_per_sec"`
	Burst      int     `json:"burst"`
}

// Sync pairs two connectors for one object kind.
type Sync struct {
	Kind      string      `json:"kind"`
	Left      string      `json:"left"`
	Right     string      `json:"right"`
	Direction string      `json:"direction"`
	Identity  Identity    `json:"identity"`
	Conflict  Conflict    `json:"conflict"`
	Fields    []FieldPair `json:"fields"`
}

// Identity configures how a record on one side is matched to its counterpart.
type Identity struct {
	Keys        []string `json:"keys"`
	OnAmbiguous string   `json:"on_ambiguous"`
}

// Conflict configures what happens when both sides changed since the last sync.
type Conflict struct {
	Default  string            `json:"default"`
	PerField map[string]string `json:"per_field,omitempty"`
}

// FieldPair maps one canonical field onto both peers.
type FieldPair struct {
	Canonical string `json:"canonical"`
	Left      string `json:"left"`
	Right     string `json:"right"`
	Transform string `json:"transform,omitempty"`
	// Direction narrows this one field relative to the sync's direction.
	Direction string `json:"direction,omitempty"`
}

var (
	validDirections  = []string{"push", "pull", "bidirectional"}
	validAmbiguous   = []string{"review", "create", "skip"}
	validConflicts   = []string{"left_wins", "right_wins", "newest_wins", "field_level", "review"}
	validLogLevels   = []string{"debug", "info", "warn", "error"}
	knownTransforms  = []string{"", "email_normalize", "domain_normalize", "trim", "lowercase"}
	errNoConnectors  = errors.New("connectors: at least two connectors are required")
	errNoSyncsAtAll  = errors.New("syncs: at least one sync is required")
	errNoFieldsInAny = errors.New("fields: a sync with no fields would write nothing")
)

// Load reads and validates a config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	// An unknown key is almost always a typo in a field name, and a typo in a
	// config that drives writes into a CRM should stop the process.
	dec.DisallowUnknownFields()

	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8080"
	}
	if c.StateDir == "" {
		c.StateDir = "./state"
	}
	if c.Workers == 0 {
		c.Workers = 4
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 6
	}
	for i := range c.Connectors {
		if c.Connectors[i].EchoWindow == 0 {
			c.Connectors[i].EchoWindow = Duration(5 * time.Minute)
		}
		if c.Connectors[i].Overlap == 0 {
			c.Connectors[i].Overlap = Duration(2 * time.Minute)
		}
		if c.Connectors[i].RateLimit.RatePerSec == 0 {
			c.Connectors[i].RateLimit.RatePerSec = 5
		}
		if c.Connectors[i].RateLimit.Burst == 0 {
			c.Connectors[i].RateLimit.Burst = 10
		}
	}
	for i := range c.Syncs {
		if c.Syncs[i].Direction == "" {
			c.Syncs[i].Direction = "bidirectional"
		}
		if c.Syncs[i].Identity.OnAmbiguous == "" {
			c.Syncs[i].Identity.OnAmbiguous = "review"
		}
		if c.Syncs[i].Conflict.Default == "" {
			c.Syncs[i].Conflict.Default = "review"
		}
	}
}

// Validate reports every structural problem it can find without network access.
func (c *Config) Validate() error {
	var errs []error

	if !oneOf(c.LogLevel, validLogLevels) {
		errs = append(errs, fmt.Errorf("log_level: %q is not one of %s", c.LogLevel, strings.Join(validLogLevels, ", ")))
	}
	if c.Workers < 1 {
		errs = append(errs, fmt.Errorf("workers: must be at least 1, got %d", c.Workers))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("max_attempts: must be at least 1, got %d", c.MaxAttempts))
	}
	if len(c.Connectors) < 2 {
		errs = append(errs, errNoConnectors)
	}

	seen := make(map[string]bool, len(c.Connectors))
	for i, conn := range c.Connectors {
		where := fmt.Sprintf("connectors[%d]", i)
		switch {
		case conn.Name == "":
			errs = append(errs, fmt.Errorf("%s.name: must not be empty", where))
		case seen[conn.Name]:
			errs = append(errs, fmt.Errorf("%s.name: %q is used twice", where, conn.Name))
		default:
			seen[conn.Name] = true
		}
		if conn.Driver == "" {
			errs = append(errs, fmt.Errorf("%s.driver: must not be empty", where))
		}
		if conn.EchoWindow.D() <= 0 {
			errs = append(errs, fmt.Errorf("%s.echo_window: must be positive, got %s", where, conn.EchoWindow.D()))
		}
		if conn.Overlap.D() < 0 {
			errs = append(errs, fmt.Errorf("%s.watermark_overlap: must not be negative, got %s", where, conn.Overlap.D()))
		}
		if conn.RateLimit.RatePerSec <= 0 {
			errs = append(errs, fmt.Errorf("%s.rate_limit.rate_per_sec: must be positive, got %v", where, conn.RateLimit.RatePerSec))
		}
		if conn.RateLimit.Burst < 1 {
			errs = append(errs, fmt.Errorf("%s.rate_limit.burst: must be at least 1, got %d", where, conn.RateLimit.Burst))
		}
	}

	if len(c.Syncs) == 0 {
		errs = append(errs, errNoSyncsAtAll)
	}
	for i, s := range c.Syncs {
		errs = append(errs, s.validate(fmt.Sprintf("syncs[%d]", i), seen)...)
	}

	return errors.Join(errs...)
}

func (s Sync) validate(where string, connectors map[string]bool) []error {
	var errs []error

	if s.Kind == "" {
		errs = append(errs, fmt.Errorf("%s.kind: must not be empty", where))
	}
	for label, name := range map[string]string{"left": s.Left, "right": s.Right} {
		if name == "" {
			errs = append(errs, fmt.Errorf("%s.%s: must name a connector", where, label))
			continue
		}
		if !connectors[name] {
			errs = append(errs, fmt.Errorf("%s.%s: %q is not a declared connector", where, label, name))
		}
	}
	if s.Left != "" && s.Left == s.Right {
		errs = append(errs, fmt.Errorf("%s: left and right are both %q; a sync needs two distinct connectors", where, s.Left))
	}
	if !oneOf(s.Direction, validDirections) {
		errs = append(errs, fmt.Errorf("%s.direction: %q is not one of %s", where, s.Direction, strings.Join(validDirections, ", ")))
	}
	if !oneOf(s.Identity.OnAmbiguous, validAmbiguous) {
		errs = append(errs, fmt.Errorf("%s.identity.on_ambiguous: %q is not one of %s", where, s.Identity.OnAmbiguous, strings.Join(validAmbiguous, ", ")))
	}
	if !oneOf(s.Conflict.Default, validConflicts) {
		errs = append(errs, fmt.Errorf("%s.conflict.default: %q is not one of %s", where, s.Conflict.Default, strings.Join(validConflicts, ", ")))
	}
	if len(s.Fields) == 0 {
		errs = append(errs, fmt.Errorf("%s.%w", where, errNoFieldsInAny))
	}

	canonical := make(map[string]bool, len(s.Fields))
	for j, f := range s.Fields {
		fw := fmt.Sprintf("%s.fields[%d]", where, j)
		switch {
		case f.Canonical == "":
			errs = append(errs, fmt.Errorf("%s.canonical: must not be empty", fw))
		case canonical[f.Canonical]:
			errs = append(errs, fmt.Errorf("%s.canonical: %q is mapped twice in the same sync", fw, f.Canonical))
		default:
			canonical[f.Canonical] = true
		}
		if f.Left == "" {
			errs = append(errs, fmt.Errorf("%s.left: must name a field on %q", fw, s.Left))
		}
		if f.Right == "" {
			errs = append(errs, fmt.Errorf("%s.right: must name a field on %q", fw, s.Right))
		}
		if !oneOf(f.Transform, knownTransforms) {
			errs = append(errs, fmt.Errorf("%s.transform: %q is not a known transform", fw, f.Transform))
		}
		if f.Direction != "" && !oneOf(f.Direction, validDirections) {
			errs = append(errs, fmt.Errorf("%s.direction: %q is not one of %s", fw, f.Direction, strings.Join(validDirections, ", ")))
		}
	}

	// A per-field conflict policy for a field this sync does not map is dead
	// config, and dead config is usually a rename that was only half applied.
	for field, policy := range s.Conflict.PerField {
		if !canonical[field] {
			errs = append(errs, fmt.Errorf("%s.conflict.per_field: %q is not a mapped canonical field", where, field))
		}
		if !oneOf(policy, validConflicts) {
			errs = append(errs, fmt.Errorf("%s.conflict.per_field[%s]: %q is not one of %s", where, field, policy, strings.Join(validConflicts, ", ")))
		}
	}

	return errs
}

// CanonicalNames returns the sync's canonical field names, which is exactly
// the set SnapshotHash should be computed over.
func (s Sync) CanonicalNames() []string {
	names := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		names = append(names, f.Canonical)
	}
	return names
}

func oneOf(v string, allowed []string) bool {
	for _, a := range allowed {
		if v == a {
			return true
		}
	}
	return false
}
