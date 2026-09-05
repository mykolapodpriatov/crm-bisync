package driver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/config"
	"crm-bisync/internal/connector"
	"crm-bisync/internal/connector/fake"
)

func init() {
	Register("fake", buildFake)
}

// fixture is the on-disk shape of a fake CRM.
//
// It exists so that plan and the convergence harness can be run against
// something concrete without an account anywhere, which is what lets the
// README show real output rather than describe it.
type fixture struct {
	Objects map[string]fixtureObject `json:"objects"`
}

type fixtureObject struct {
	// Fields is the schema. Mapping validation checks against it, so a
	// fixture that forgets a field fails the same way a renamed CRM property
	// would.
	Fields []fixtureField `json:"fields"`
	// Records are the rows the peer starts with.
	Records []fixtureRecord `json:"records"`
}

type fixtureField struct {
	Name     string `json:"name"`
	Type     string `json:"type,omitempty"`
	ReadOnly bool   `json:"read_only,omitempty"`
	Required bool   `json:"required,omitempty"`
}

type fixtureRecord struct {
	ID     string         `json:"id"`
	Fields map[string]any `json:"fields"`
}

// buildFake constructs an in-memory CRM from configuration.
//
// Recognised options: file (a fixture path), webhook_secret, and the
// capability flags native_idempotency, webhooks, soft_delete, etags and
// timestamp_granularity_seconds.
func buildFake(cfg config.Connector, c clock.Clock) (connector.Connector, error) {
	caps := connector.Caps{
		NativeIdempotency: boolOption(cfg, "native_idempotency", true),
		Webhooks:          boolOption(cfg, "webhooks", true),
		SoftDelete:        boolOption(cfg, "soft_delete", false),
		ETags:             boolOption(cfg, "etags", true),
		ModifiedAtIsExact: true,
	}

	granularity, err := intOption(cfg, "timestamp_granularity_seconds")
	if err != nil {
		return nil, err
	}
	faults := fake.Faults{}
	if granularity > 0 {
		faults.TimestampGranularity = time.Duration(granularity) * time.Second
		caps.ModifiedAtIsExact = false
	}

	schemas := map[string]connector.Schema{}
	var fx fixture
	if path := cfg.Options["file"]; path != "" {
		if !filepath.IsAbs(path) && cfg.BaseDir != "" {
			path = filepath.Join(cfg.BaseDir, path)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read fixture: %w", err)
		}
		if err := json.Unmarshal(raw, &fx); err != nil {
			return nil, fmt.Errorf("parse fixture %s: %w", path, err)
		}
		for kind, object := range fx.Objects {
			schema := connector.Schema{Kind: kind}
			for _, f := range object.Fields {
				schema.Fields = append(schema.Fields, connector.FieldSpec{
					Name:     f.Name,
					Type:     f.Type,
					ReadOnly: f.ReadOnly,
					Required: f.Required,
				})
			}
			schemas[kind] = schema
		}
	}

	secret := cfg.Options["webhook_secret"]
	if secret == "" {
		secret = cfg.Name + "-secret"
	}

	f := fake.New(fake.Options{
		Name:    cfg.Name,
		Clock:   c,
		Caps:    caps,
		Secret:  secret,
		Schemas: schemas,
		Faults:  faults,
	})

	// Seeded through the external-edit path, because that is what a record
	// that was already there looks like: not something this engine wrote.
	for kind, object := range fx.Objects {
		for _, rec := range object.Records {
			f.ExternalUpsert(kind, rec.ID, rec.Fields)
		}
	}
	return f, nil
}

func boolOption(cfg config.Connector, name string, fallback bool) bool {
	raw, ok := cfg.Options[name]
	if !ok {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}

func intOption(cfg config.Connector, name string) (int, error) {
	raw, ok := cfg.Options[name]
	if !ok || raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("option %s: %q is not a number", name, raw)
	}
	return value, nil
}
