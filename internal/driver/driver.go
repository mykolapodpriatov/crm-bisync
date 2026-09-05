// Package driver turns a configured connector into a live one.
//
// It exists so that the commands do not import every connector, and so that
// adding one is a registration rather than an edit to a switch statement in
// main.
package driver

import (
	"fmt"
	"sort"
	"sync"

	"crm-bisync/internal/clock"
	"crm-bisync/internal/config"
	"crm-bisync/internal/connector"
)

// Factory builds one connector from its configuration.
type Factory func(cfg config.Connector, c clock.Clock) (connector.Connector, error)

var (
	mu      sync.RWMutex
	drivers = map[string]Factory{}
)

// Register adds a driver. It panics on a duplicate, because two drivers under
// one name is a build-time mistake and finding it at run time would mean
// finding it in whichever order the linker happened to choose.
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := drivers[name]; exists {
		panic("driver: " + name + " is registered twice")
	}
	drivers[name] = f
}

// Names lists the registered drivers, sorted.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()

	out := make([]string, 0, len(drivers))
	for name := range drivers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Build makes one connector.
func Build(cfg config.Connector, c clock.Clock) (connector.Connector, error) {
	mu.RLock()
	factory, ok := drivers[cfg.Driver]
	mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf(
			"connector %q asks for driver %q, which is not built in; the drivers are: %v",
			cfg.Name, cfg.Driver, Names())
	}
	conn, err := factory(cfg, c)
	if err != nil {
		return nil, fmt.Errorf("connector %q: %w", cfg.Name, err)
	}
	return conn, nil
}

// BuildAll makes every connector a configuration names.
func BuildAll(cfg *config.Config, c clock.Clock) (map[string]connector.Connector, error) {
	out := make(map[string]connector.Connector, len(cfg.Connectors))
	for _, cc := range cfg.Connectors {
		conn, err := Build(cc, c)
		if err != nil {
			return nil, err
		}
		out[cc.Name] = conn
	}
	return out, nil
}
