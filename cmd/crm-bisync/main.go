// Command crm-bisync keeps two CRMs in sync in both directions.
//
// The subcommands are deliberately split so that the read-only ones can be run
// against production before the writing one ever is:
//
//	doctor  verify credentials, scopes, remote schema and mapping validity
//	plan    print every write that would happen, change nothing
//	run     the daemon
//	dlq     inspect and replay dead-lettered work
//	review  resolve ambiguous matches and conflicts
//	chaos   run the fault-injection harness against in-memory fake CRMs
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"crm-bisync/internal/config"
)

// version is stamped at build time with -ldflags "-X main.version=...".
var version = "dev"

// errNotImplemented marks a subcommand whose milestone has not landed yet. It
// exits 3 rather than 1 so a script can tell "not built" from "failed".
var errNotImplemented = errors.New("not implemented yet")

const usage = `crm-bisync %s

Usage:
  crm-bisync <command> [flags]

Commands:
  doctor    Verify credentials, scopes, remote schema and mapping validity
  plan      Print every write that would happen, without performing any
  run       Start pollers, the webhook receiver, workers and /metrics
  dlq       List, inspect and replay dead-lettered work items
  review    List and resolve ambiguous matches and conflicts
  chaos     Run the fault-injection harness against in-memory fake CRMs
  version   Print the version and exit

Run "crm-bisync <command> -h" for the flags of a command.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "crm-bisync: %v\n", err)
		if errors.Is(err, errNotImplemented) {
			os.Exit(3)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Printf(usage, version)
		return nil
	}

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "version", "-version", "--version":
		fmt.Println(version)
		return nil
	case "help", "-h", "--help":
		fmt.Printf(usage, version)
		return nil
	case "doctor", "plan", "run", "dlq", "review", "chaos":
		return dispatch(cmd, rest)
	default:
		return fmt.Errorf("unknown command %q; run \"crm-bisync help\"", cmd)
	}
}

// dispatch parses the flags a command shares and hands off to its milestone
// implementation. Until that lands it reports errNotImplemented, but it still
// parses and validates the config, so the flag surface and the config loader
// are exercised from day one instead of being wired up blind later.
func dispatch(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	path := fs.String("config", "config.json", "path to the configuration file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}

	names := make([]string, 0, len(cfg.Connectors))
	for _, c := range cfg.Connectors {
		names = append(names, c.Name)
	}
	fmt.Printf("config %s is valid: %d connectors (%s), %d syncs\n",
		*path, len(cfg.Connectors), strings.Join(names, ", "), len(cfg.Syncs))

	return fmt.Errorf("%s: %w", cmd, errNotImplemented)
}
