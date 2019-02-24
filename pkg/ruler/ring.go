package ruler

import (
	"github.com/cortexproject/cortex/pkg/ring"
)

// CreateRulerRingConfig modifies a given ring config and repurposes it for use within
// the ruler
func CreateRulerRingConfig(cfg ring.Config) ring.Config {
	// Copy the consul config struct and ensure a different prefix is used
	consulCfg := cfg.Consul
	consulCfg.Prefix = "rulers/"

	return ring.Config{
		Consul:            consulCfg,
		Store:             cfg.Store,
		HeartbeatTimeout:  cfg.HeartbeatTimeout,
		ReplicationFactor: 1, // Ruler will never use a RF > 1
	}
}
