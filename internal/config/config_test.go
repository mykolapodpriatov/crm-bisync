package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const minimal = `{
  "connectors": [
    {"name": "hubspot", "driver": "hubspot"},
    {"name": "twenty",  "driver": "twenty"}
  ],
  "syncs": [
    {
      "kind": "contact",
      "left": "hubspot",
      "right": "twenty",
      "fields": [{"canonical": "email", "left": "email", "right": "emails.primaryEmail"}]
    }
  ]
}`

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func mutate(t *testing.T, base string, edit func(m map[string]any)) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(base), &m); err != nil {
		t.Fatalf("unmarshal base config: %v", err)
	}
	edit(m)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal mutated config: %v", err)
	}
	return string(out)
}

func TestLoadAppliesDefaults(t *testing.T) {
	c, err := Load(write(t, minimal))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Workers != 4 || c.MaxAttempts != 6 {
		t.Fatalf("workers=%d max_attempts=%d, want 4 and 6", c.Workers, c.MaxAttempts)
	}
	if c.LogLevel != "info" || c.Listen == "" || c.StateDir == "" {
		t.Fatalf("unset top-level defaults: %+v", c)
	}
	if got := c.Connectors[0].EchoWindow.D(); got != 5*time.Minute {
		t.Fatalf("echo_window default = %s, want 5m", got)
	}
	if got := c.Connectors[0].Overlap.D(); got != 2*time.Minute {
		t.Fatalf("watermark_overlap default = %s, want 2m", got)
	}
	// Defaulting the conflict policy to anything that writes would mean a
	// half-written config silently picks a winner. It must default to review.
	if c.Syncs[0].Conflict.Default != "review" {
		t.Fatalf("conflict default = %q, want review", c.Syncs[0].Conflict.Default)
	}
	if c.Syncs[0].Identity.OnAmbiguous != "review" {
		t.Fatalf("on_ambiguous default = %q, want review", c.Syncs[0].Identity.OnAmbiguous)
	}
}

func TestDurationRoundTrips(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"90s"`), &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if d.D() != 90*time.Second {
		t.Fatalf("parsed %s, want 1m30s", d.D())
	}
	out, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != `"1m30s"` {
		t.Fatalf("marshalled %s, want \"1m30s\"", out)
	}
}

func TestDurationRejectsGarbage(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"5 minutes"`), &d); err == nil {
		t.Fatal("accepted an unparsable duration")
	}
	if err := json.Unmarshal([]byte(`300`), &d); err == nil {
		t.Fatal("accepted a bare number as a duration")
	}
}

// A typo in a key name is the most likely way a config silently stops doing
// what its author meant, so unknown keys must be an error.
func TestLoadRejectsUnknownKeys(t *testing.T) {
	body := mutate(t, minimal, func(m map[string]any) { m["worker"] = 8 })
	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("accepted an unknown top-level key")
	}
	if !strings.Contains(err.Error(), "worker") {
		t.Fatalf("error does not name the offending key: %v", err)
	}
}

func TestValidateRejectsUnknownConnectorReference(t *testing.T) {
	body := mutate(t, minimal, func(m map[string]any) {
		m["syncs"].([]any)[0].(map[string]any)["right"] = "pipedrive"
	})
	_, err := Load(write(t, body))
	if err == nil || !strings.Contains(err.Error(), "pipedrive") {
		t.Fatalf("want an error naming pipedrive, got %v", err)
	}
}

func TestValidateRejectsSyncingAConnectorWithItself(t *testing.T) {
	body := mutate(t, minimal, func(m map[string]any) {
		m["syncs"].([]any)[0].(map[string]any)["right"] = "hubspot"
	})
	_, err := Load(write(t, body))
	if err == nil || !strings.Contains(err.Error(), "two distinct connectors") {
		t.Fatalf("want a distinct-connector error, got %v", err)
	}
}

func TestValidateRejectsDuplicateConnectorNames(t *testing.T) {
	body := mutate(t, minimal, func(m map[string]any) {
		m["connectors"].([]any)[1].(map[string]any)["name"] = "hubspot"
	})
	_, err := Load(write(t, body))
	if err == nil || !strings.Contains(err.Error(), "used twice") {
		t.Fatalf("want a duplicate-name error, got %v", err)
	}
}

func TestValidateRejectsDuplicateCanonicalField(t *testing.T) {
	body := mutate(t, minimal, func(m map[string]any) {
		sync := m["syncs"].([]any)[0].(map[string]any)
		sync["fields"] = []any{
			map[string]any{"canonical": "email", "left": "email", "right": "a"},
			map[string]any{"canonical": "email", "left": "work_email", "right": "b"},
		}
	})
	_, err := Load(write(t, body))
	if err == nil || !strings.Contains(err.Error(), "mapped twice") {
		t.Fatalf("want a duplicate-canonical error, got %v", err)
	}
}

// A per-field policy naming a field the sync does not map is usually a rename
// that was only half applied, which would silently fall back to the default.
func TestValidateRejectsPerFieldPolicyForUnmappedField(t *testing.T) {
	body := mutate(t, minimal, func(m map[string]any) {
		sync := m["syncs"].([]any)[0].(map[string]any)
		sync["conflict"] = map[string]any{
			"default":   "newest_wins",
			"per_field": map[string]any{"lifecyclestage": "right_wins"},
		}
	})
	_, err := Load(write(t, body))
	if err == nil || !strings.Contains(err.Error(), "lifecyclestage") {
		t.Fatalf("want an error naming the unmapped field, got %v", err)
	}
}

func TestValidateRejectsUnknownEnums(t *testing.T) {
	cases := map[string]func(m map[string]any){
		"direction": func(m map[string]any) {
			m["syncs"].([]any)[0].(map[string]any)["direction"] = "sideways"
		},
		"conflict.default": func(m map[string]any) {
			m["syncs"].([]any)[0].(map[string]any)["conflict"] = map[string]any{"default": "coin_flip"}
		},
		"on_ambiguous": func(m map[string]any) {
			m["syncs"].([]any)[0].(map[string]any)["identity"] = map[string]any{"on_ambiguous": "merge"}
		},
		"transform": func(m map[string]any) {
			f := m["syncs"].([]any)[0].(map[string]any)["fields"].([]any)[0].(map[string]any)
			f["transform"] = "shout"
		},
		"log_level": func(m map[string]any) { m["log_level"] = "verbose" },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(write(t, mutate(t, minimal, edit))); err == nil {
				t.Fatalf("accepted an invalid %s", name)
			}
		})
	}
}

func TestValidateRejectsSyncWithNoFields(t *testing.T) {
	body := mutate(t, minimal, func(m map[string]any) {
		m["syncs"].([]any)[0].(map[string]any)["fields"] = []any{}
	})
	if _, err := Load(write(t, body)); err == nil {
		t.Fatal("accepted a sync that maps no fields")
	}
}

func TestValidateRejectsSingleConnector(t *testing.T) {
	body := mutate(t, minimal, func(m map[string]any) {
		m["connectors"] = []any{m["connectors"].([]any)[0]}
	})
	if _, err := Load(write(t, body)); err == nil {
		t.Fatal("accepted a config with one connector")
	}
}

// Validate reports everything it found, not just the first problem, so one run
// of doctor can fix a whole config.
func TestValidateReportsEveryProblemAtOnce(t *testing.T) {
	body := mutate(t, minimal, func(m map[string]any) {
		m["log_level"] = "verbose"
		m["workers"] = -1
		m["syncs"].([]any)[0].(map[string]any)["direction"] = "sideways"
	})
	_, err := Load(write(t, body))
	if err == nil {
		t.Fatal("accepted a config with three problems")
	}
	for _, want := range []string{"log_level", "workers", "direction"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error is missing %q: %v", want, err)
		}
	}
}

func TestCanonicalNames(t *testing.T) {
	c, err := Load(write(t, minimal))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := c.Syncs[0].CanonicalNames()
	if len(got) != 1 || got[0] != "email" {
		t.Fatalf("CanonicalNames() = %v, want [email]", got)
	}
}

func TestLoadReportsMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("Load succeeded for a file that does not exist")
	}
}

// The example config is the first thing anyone copies, so a change to the
// schema that forgets to update it should fail CI rather than fail for a user.
func TestShippedExampleConfigIsValid(t *testing.T) {
	if _, err := Load(filepath.Join("..", "..", "config.example.json")); err != nil {
		t.Fatalf("config.example.json does not load: %v", err)
	}
}
