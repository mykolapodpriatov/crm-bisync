package chaos

import (
	"sort"
	"time"

	"crm-bisync/internal/conflict"
	"crm-bisync/internal/connector/fake"
)

// Scenario is a named set of things going wrong.
type Scenario struct {
	Name        string
	Description string
	Options     Options
}

// Scenarios are the shapes worth running, each named after the failure it is
// about rather than after the knob it turns.
func Scenarios() []Scenario {
	return []Scenario{
		{
			Name:        "calm",
			Description: "Nothing goes wrong. The baseline: if this fails, none of the others mean anything.",
			Options:     Options{Policy: conflict.LeftWins},
		},
		{
			Name:        "redelivery",
			Description: "Every webhook arrives twice, out of order, and some carry no record body.",
			Options: Options{
				Policy: conflict.LeftWins,
				Faults: fake.Faults{
					DuplicateWebhooks: true,
					ShuffleWebhooks:   true,
					NotificationOnly:  true,
				},
			},
		},
		{
			Name:        "flaky",
			Description: "One call in ten fails, and one in seven is rate limited without a Retry-After.",
			Options: Options{
				Policy: conflict.LeftWins,
				Faults: fake.Faults{
					ServerErrorRate: 0.1,
					RateLimitEvery:  7,
				},
			},
		},
		{
			Name:        "bad-clocks",
			Description: "Timestamps are rounded to the minute and the peers' clocks disagree.",
			Options: Options{
				Policy: conflict.LeftWins,
				Faults: fake.Faults{
					TimestampGranularity: time.Minute,
					ClockOffset:          -90 * time.Second,
				},
			},
		},
		{
			Name:        "paged",
			Description: "Peers return two records per page, so every poll is several round trips.",
			Options: Options{
				Policy: conflict.LeftWins,
				Faults: fake.Faults{PageSize: 2},
			},
		},
		{
			Name:        "everything",
			Description: "All of the above at once, with the engine restarted halfway through.",
			Options: Options{
				Policy:  conflict.LeftWins,
				Restart: true,
				Faults: fake.Faults{
					DuplicateWebhooks:    true,
					ShuffleWebhooks:      true,
					NotificationOnly:     true,
					ServerErrorRate:      0.1,
					RateLimitEvery:       7,
					TimestampGranularity: time.Minute,
					ClockOffset:          -90 * time.Second,
					PageSize:             2,
				},
			},
		},
		{
			Name:        "undecidable",
			Description: "The same faults, under newest_wins, where edits inside the clock tolerance cannot be resolved and must be queued instead.",
			Options: Options{
				Policy:  conflict.NewestWins,
				Restart: true,
				Faults: fake.Faults{
					DuplicateWebhooks:    true,
					ServerErrorRate:      0.1,
					TimestampGranularity: time.Minute,
				},
			},
		},
	}
}

// ScenarioNamed looks one up.
func ScenarioNamed(name string) (Scenario, bool) {
	for _, s := range Scenarios() {
		if s.Name == name {
			return s, true
		}
	}
	return Scenario{}, false
}

// ScenarioNames lists them, sorted.
func ScenarioNames() []string {
	names := make([]string, 0)
	for _, s := range Scenarios() {
		names = append(names, s.Name)
	}
	sort.Strings(names)
	return names
}
