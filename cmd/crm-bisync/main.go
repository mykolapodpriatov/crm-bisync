// Command crm-bisync keeps two CRMs in sync in both directions.
//
// The subcommands are split so that the read-only ones can be run against
// production before the writing one ever is:
//
//	doctor  verify credentials, remote schema, mapping validity and progress
//	plan    print every write that would happen, change nothing
//	run     the daemon
//	dlq     inspect and replay dead-lettered work
//	review  inspect decisions the engine refused to make
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/config"
	"crm-bisync/internal/driver"
	"crm-bisync/internal/engine"
	"crm-bisync/internal/report"
	"crm-bisync/internal/store"
)

// version is stamped at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `crm-bisync %s

Usage:
  crm-bisync <command> [flags]

Commands:
  doctor    Verify credentials, remote schema, mapping validity and progress
  plan      Print every write that would happen, without performing any
  run       Start pollers, the webhook receiver and the worker pool
  dlq       List, show, replay or discard dead-lettered work
  review    List, show or discard decisions waiting for a person
  version   Print the version and exit

Run "crm-bisync <command> -h" for the flags of a command.
`

// exit codes, so a script can tell the cases apart.
const (
	exitOK       = 0
	exitError    = 1
	exitFindings = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr *os.File) int {
	if len(args) == 0 {
		fmt.Fprintf(stdout, usage, version)
		return exitOK
	}

	cmd, rest := args[0], args[1:]
	var err error
	var code int

	switch cmd {
	case "version", "-version", "--version":
		fmt.Fprintln(stdout, version)
		return exitOK
	case "help", "-h", "--help":
		fmt.Fprintf(stdout, usage, version)
		return exitOK
	case "doctor":
		code, err = doctor(rest, stdout)
	case "plan":
		code, err = plan(rest, stdout)
	case "run":
		code, err = daemon(rest, stderr)
	case "dlq":
		code, err = queueCommand(store.CollDLQ, rest, stdout)
	case "review":
		code, err = queueCommand(store.CollReview, rest, stdout)
	default:
		err = fmt.Errorf("unknown command %q; run \"crm-bisync help\"", cmd)
	}

	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		fmt.Fprintf(stderr, "crm-bisync: %v\n", err)
		return exitError
	}
	return code
}

// session is everything a command needs, built once from the config file.
type session struct {
	cfg    *config.Config
	store  *store.File
	clock  clock.Clock
	engine *engine.Engine
	opts   engine.Options
}

func (s *session) Close() {
	if s.store != nil {
		_ = s.store.Close()
	}
}

// open loads the configuration, opens the state directory and builds the
// connectors. Everything that can fail without touching a CRM fails here.
func open(path string, logger *slog.Logger) (*session, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}

	c := clock.Real{}
	connectors, err := driver.BuildAll(cfg, c)
	if err != nil {
		return nil, err
	}

	state, err := store.OpenFile(cfg.StateDir, store.FileOptions{Clock: c})
	if err != nil {
		return nil, err
	}

	opts := engine.Options{
		Config:     cfg,
		Store:      state,
		Clock:      c,
		Logger:     logger,
		Connectors: connectors,
	}
	e, err := engine.New(opts)
	if err != nil {
		_ = state.Close()
		return nil, err
	}
	return &session{cfg: cfg, store: state, clock: c, engine: e, opts: opts}, nil
}

// flags builds the flag set every command shares.
func flags(name string) (*flag.FlagSet, *string, *bool) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	path := fs.String("config", "config.json", "path to the configuration file")
	asJSON := fs.Bool("json", false, "print machine-readable output")
	return fs, path, asJSON
}

func doctor(args []string, out *os.File) (int, error) {
	fs, path, asJSON := flags("doctor")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}

	s, err := open(*path, quietLogger())
	if err != nil {
		return exitError, err
	}
	defer s.Close()

	diagnosis := s.engine.Doctor(context.Background())
	if *asJSON {
		if err := writeJSON(out, diagnosis); err != nil {
			return exitError, err
		}
	} else {
		report.Doctor(out, diagnosis)
	}

	if !diagnosis.OK() {
		return exitFindings, nil
	}
	return exitOK, nil
}

func plan(args []string, out *os.File) (int, error) {
	fs, path, asJSON := flags("plan")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}

	s, err := open(*path, quietLogger())
	if err != nil {
		return exitError, err
	}
	defer s.Close()

	result, err := engine.Plan(context.Background(), s.opts)
	if err != nil {
		return exitError, err
	}

	if *asJSON {
		if err := writeJSON(out, result); err != nil {
			return exitError, err
		}
	} else {
		report.Plan(out, result)
	}

	// A plan that would change something exits non-zero, so that it can gate
	// a deployment the way a diff does.
	if !result.Empty() {
		return exitFindings, nil
	}
	return exitOK, nil
}

func daemon(args []string, stderr *os.File) (int, error) {
	fs, path, _ := flags("run")
	interval := fs.Duration("interval", time.Minute, "how often to poll each peer")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}

	s, err := open(*path, logger(stderr))
	if err != nil {
		return exitError, err
	}
	defer s.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := s.engine.Start(ctx); err != nil {
		return exitError, err
	}
	defer s.engine.Stop()

	server := &http.Server{
		Addr:              s.cfg.Listen,
		Handler:           s.engine.WebhookHandler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(stderr, "crm-bisync: webhook listener: %v\n", err)
			stop()
		}
	}()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		if _, err := s.engine.PollAll(ctx); err != nil {
			// One unreachable peer must not stop the daemon: its mark simply
			// does not move, and the next tick tries again.
			fmt.Fprintf(stderr, "crm-bisync: poll: %v\n", err)
		}
		if _, err := s.engine.Sweep(); err != nil {
			fmt.Fprintf(stderr, "crm-bisync: sweep: %v\n", err)
		}

		select {
		case <-ctx.Done():
			return exitOK, nil
		case <-ticker.C:
		}
	}
}

// queueCommand serves both dlq and review, which hold the same envelope.
func queueCommand(collection string, args []string, out *os.File) (int, error) {
	name := "review"
	if collection == store.CollDLQ {
		name = "dlq"
	}

	fs, path, asJSON := flags(name)
	limit := fs.Int("limit", 50, "how many items to list")
	show := fs.String("show", "", "print one item in full, by ID")
	discard := fs.String("discard", "", "remove one item, by ID")
	replay := fs.String("replay", "", "put a dead-lettered item back on the queue, by ID")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}

	s, err := open(*path, quietLogger())
	if err != nil {
		return exitError, err
	}
	defer s.Close()

	switch {
	case *discard != "":
		if err := s.engine.Discard(collection, *discard); err != nil {
			return exitError, err
		}
		fmt.Fprintf(out, "Removed %s.\n", *discard)
		return exitOK, nil

	case *replay != "":
		if collection != store.CollDLQ {
			return exitError, errors.New("only dead-lettered work can be replayed")
		}
		if err := s.engine.Replay(*replay); err != nil {
			return exitError, err
		}
		fmt.Fprintf(out, "Requeued %s. It runs on the next pass.\n", *replay)
		return exitOK, nil
	}

	items, err := s.engine.DeadLetters(*limit)
	if collection == store.CollReview {
		items, err = s.engine.Reviews(*limit)
	}
	if err != nil {
		return exitError, err
	}

	if *show != "" {
		for _, item := range items {
			if item.ID == *show {
				return exitOK, writeJSON(out, item)
			}
		}
		return exitError, fmt.Errorf("no item %q", *show)
	}

	if *asJSON {
		return exitOK, writeJSON(out, items)
	}

	if len(items) == 0 {
		fmt.Fprintf(out, "The %s is empty.\n", strings.ReplaceAll(name, "dlq", "dead-letter queue"))
		return exitOK, nil
	}
	for _, item := range items {
		fmt.Fprintf(out, "%s  %s\n  %s\n", item.ID, item.Ref.String(), item.Reason)
	}
	fmt.Fprintf(out, "\n%d item(s).\n", len(items))
	return exitFindings, nil
}

func writeJSON(out *os.File, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// quietLogger keeps the read-only commands' output to what they print, so it
// stays pipeable.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func logger(w *os.File) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
