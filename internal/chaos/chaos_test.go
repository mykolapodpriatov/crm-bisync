package chaos_test

import (
	"context"
	"testing"

	"crm-bisync/internal/chaos"
	"crm-bisync/internal/conflict"
)

// seeds is how many runs each scenario gets. A property test that runs once is
// an anecdote; the number is what turns it into a claim.
const seeds = 30

func runSeed(t *testing.T, opts chaos.Options) *chaos.Result {
	t.Helper()

	result, err := chaos.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("seed %d: %v", opts.Seed, err)
	}
	return result
}

// check runs one seed and fails with everything that broke, plus the seed,
// because a failure that cannot be replayed is a rumour.
func check(t *testing.T, opts chaos.Options) {
	t.Helper()

	result := runSeed(t, opts)
	violations := result.Verify()
	if len(violations) == 0 {
		return
	}

	t.Errorf("seed %d broke %d invariant(s); replay with: crm-bisync chaos --scenario %s --seed %d",
		opts.Seed, len(violations), t.Name(), opts.Seed)
	for _, v := range violations {
		t.Errorf("  %s", v)
	}
	t.Logf("  %d live, %d deleted, %d reviews, %d dead letters, settled after %d rounds",
		len(result.Live), len(result.Deleted), len(result.Reviews),
		len(result.DeadLetters), result.SettledAfter)
}

// TestScenarios is the flagship. Every named scenario, over many seeds, with
// every invariant checked on each.
func TestScenarios(t *testing.T) {
	for _, scenario := range chaos.Scenarios() {
		scenario := scenario
		t.Run(scenario.Name, func(t *testing.T) {
			t.Parallel()
			for seed := int64(1); seed <= seeds; seed++ {
				opts := scenario.Options
				opts.Seed = seed
				check(t, opts)
			}
		})
	}
}

// Under left_wins there is always an answer, so the engine has no excuse to
// queue anything and the two sides must agree completely.
func TestADecidablePolicyNeverQueuesADecision(t *testing.T) {
	t.Parallel()

	for seed := int64(1); seed <= seeds; seed++ {
		result := runSeed(t, chaos.Options{Seed: seed, Policy: conflict.LeftWins})
		if len(result.Reviews) != 0 {
			t.Errorf("seed %d: left_wins queued %d decisions, and it always has an answer",
				seed, len(result.Reviews))
			for _, item := range result.Reviews {
				t.Errorf("  %s: %s", item.Ref, item.Reason)
			}
		}
	}
}

// The same seed has to produce the same run, or none of the failures above can
// be replayed from the seed they print.
func TestRunsAreReproducible(t *testing.T) {
	t.Parallel()

	opts := chaos.Options{Seed: 12345, Policy: conflict.LeftWins}
	first := runSeed(t, opts)
	second := runSeed(t, opts)

	if len(first.Live) != len(second.Live) || len(first.Deleted) != len(second.Deleted) {
		t.Fatalf("the same seed produced different worlds: %d/%d then %d/%d",
			len(first.Live), len(first.Deleted), len(second.Live), len(second.Deleted))
	}
	for i := range first.Live {
		if first.Live[i] != second.Live[i] {
			t.Fatalf("live sets differ at %d: %q then %q", i, first.Live[i], second.Live[i])
		}
	}
	if first.WritesDuringEdits != second.WritesDuringEdits {
		t.Fatalf("the same seed made %d writes then %d",
			first.WritesDuringEdits, second.WritesDuringEdits)
	}
}

// Restarting the engine mid-run must change nothing that matters. Whatever was
// in the queue is lost; the watermark never moved past it, so the next poll
// reads it again.
func TestRestartingChangesNothing(t *testing.T) {
	t.Parallel()

	for seed := int64(1); seed <= seeds; seed++ {
		steady := runSeed(t, chaos.Options{Seed: seed, Policy: conflict.LeftWins})
		restarted := runSeed(t, chaos.Options{Seed: seed, Policy: conflict.LeftWins, Restart: true})

		if v := restarted.Verify(); len(v) != 0 {
			t.Errorf("seed %d broke %d invariant(s) with a restart", seed, len(v))
			for _, violation := range v {
				t.Errorf("  %s", violation)
			}
			continue
		}
		if len(steady.Live) != len(restarted.Live) {
			t.Errorf("seed %d: %d live without a restart, %d with one",
				seed, len(steady.Live), len(restarted.Live))
		}
	}
}

// Every scenario has to actually do something. One that quietly stopped
// generating edits would pass every invariant and prove nothing.
func TestScenariosDoSomething(t *testing.T) {
	t.Parallel()

	for _, scenario := range chaos.Scenarios() {
		opts := scenario.Options
		opts.Seed = 99
		result := runSeed(t, opts)

		if len(result.Live)+len(result.Deleted) == 0 {
			t.Errorf("scenario %q made no edits at all", scenario.Name)
		}
		if result.WritesDuringEdits == 0 {
			t.Errorf("scenario %q caused no writes, so it tested nothing", scenario.Name)
		}
	}
}

func TestScenarioLookup(t *testing.T) {
	if _, ok := chaos.ScenarioNamed("everything"); !ok {
		t.Fatal("the everything scenario is missing")
	}
	if _, ok := chaos.ScenarioNamed("no-such-scenario"); ok {
		t.Fatal("an unknown scenario was found")
	}
	if len(chaos.ScenarioNames()) != len(chaos.Scenarios()) {
		t.Fatal("the name list and the scenario list disagree")
	}
}
