package alertmanager

import (
	"context"
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/chunk/gcp"
)

// AlertStoreConfig configures the alertmanager backend
type AlertStoreConfig struct {
	Type string `yaml:"type"`
	GCS  gcp.GCSAlertStoreConfig
}

// RegisterFlags registers flags.
func (cfg *AlertStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.GCS.RegisterFlags(f)
	f.StringVar(&cfg.Type, "alertmanager.storage.type", "configdb", "Method to use for backend rule storage (configdb, gcs)")
}

// NewAlertStore returns a new rule storage backend poller and store
func NewAlertStore(cfg AlertStoreConfig) (AlertStore, error) {
	switch cfg.Type {
	case "gcs":
		return gcp.NewGCSAlertStoreClient(context.Background(), cfg.GCS)
	default:
		return nil, fmt.Errorf("Unrecognized rule storage mode %v, choose one of: configdb, gcs", cfg.Type)
	}
}
