package memory

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/configs"
)

// DB is an in-memory database for testing, and local development
type DB struct {
	cfgs map[string]entry
	id   uint
}

type entry struct {
	view    configs.View
	updated time.Time
}

// New creates a new in-memory database
func New(_, _ string) (*DB, error) {
	return &DB{
		cfgs: map[string]entry{},
		id:   0,
	}, nil
}

// GetConfig gets the user's configuration.
func (d *DB) GetConfig(userID string) (configs.View, error) {
	c, ok := d.cfgs[userID]
	if !ok {
		return configs.View{}, sql.ErrNoRows
	}
	return c.view, nil
}

// SetConfig sets configuration for a user.
func (d *DB) SetConfig(userID string, cfg configs.Config) error {
	if !cfg.RulesConfig.FormatVersion.IsValid() {
		return fmt.Errorf("invalid rule format version %v", cfg.RulesConfig.FormatVersion)
	}
	d.cfgs[userID] = entry{
		configs.View{Config: cfg, ID: configs.ID(d.id)},
		time.Now(),
	}
	d.id++
	return nil
}

// GetAllConfigs gets all of the configs.
func (d *DB) GetAllConfigs() (map[string]configs.View, error) {
	configMap := map[string]configs.View{}
	for user, cfg := range d.cfgs {
		configMap[user] = cfg.view
	}
	return configMap, nil
}

// GetConfigs gets all of the configs that have changed recently.
func (d *DB) GetConfigs(since time.Time) (map[string]configs.View, error) {
	cfgs := map[string]configs.View{}
	fmt.Println(since.Unix())
	for user, cfg := range d.cfgs {
		fmt.Printf("id: %v  time:%d\n", cfg.view.ID, cfg.updated.Unix())
		if cfg.updated.After(since) {
			cfgs[user] = cfg.view
		}
	}
	return cfgs, nil
}

// SetDeletedAtConfig sets a deletedAt for configuration
// by adding a single new row with deleted_at set
// the same as SetConfig is actually insert
func (d *DB) SetDeletedAtConfig(userID string, deletedAt time.Time) error {
	cv, err := d.GetConfig(userID)
	if err != nil {
		return err
	}
	cv.DeletedAt = deletedAt
	cv.ID = configs.ID(d.id)
	d.cfgs[userID] = entry{
		cv,
		time.Now(),
	}
	d.id++
	return nil
}

// DeactivateConfig deactivates configuration for a user by creating new configuration with DeletedAt set to now
func (d *DB) DeactivateConfig(userID string) error {
	return d.SetDeletedAtConfig(userID, time.Now())
}

// RestoreConfig restores deactivated configuration for a user by creating new configuration with empty DeletedAt
func (d *DB) RestoreConfig(userID string) error {
	return d.SetDeletedAtConfig(userID, time.Time{})
}

// Close finishes using the db. Noop.
func (d *DB) Close() error {
	return nil
}

// GetRulesConfig gets the rules config for a user.
func (d *DB) GetRulesConfig(userID string) (configs.VersionedRulesConfig, error) {
	c, ok := d.cfgs[userID]
	if !ok {
		return configs.VersionedRulesConfig{}, sql.ErrNoRows
	}
	cfg := c.view.GetVersionedRulesConfig()
	if cfg == nil {
		return configs.VersionedRulesConfig{}, sql.ErrNoRows
	}
	return *cfg, nil
}

// SetRulesConfig sets the rules config for a user.
func (d *DB) SetRulesConfig(userID string, oldConfig, newConfig configs.RulesConfig) (bool, error) {
	c, ok := d.cfgs[userID]
	if !ok {
		return true, d.SetConfig(userID, configs.Config{RulesConfig: newConfig})
	}
	if !oldConfig.Equal(c.view.Config.RulesConfig) {
		return false, nil
	}
	return true, d.SetConfig(userID, configs.Config{
		AlertmanagerConfig: c.view.Config.AlertmanagerConfig,
		RulesConfig:        newConfig,
	})
}

// GetAllRulesConfigs gets the rules configs for all users that have them.
func (d *DB) GetAllRulesConfigs() (map[string]configs.VersionedRulesConfig, error) {
	cfgs := map[string]configs.VersionedRulesConfig{}
	for user, c := range d.cfgs {
		cfg := c.view.GetVersionedRulesConfig()
		if cfg != nil {
			cfgs[user] = *cfg
		}
	}
	return cfgs, nil
}

// GetRulesConfigs gets the rules configs that have changed
// since the given config version.
func (d *DB) GetRulesConfigs(since time.Time) (map[string]configs.VersionedRulesConfig, error) {
	cfgs := map[string]configs.VersionedRulesConfig{}
	for user, c := range d.cfgs {
		if !c.updated.After(since) {
			continue
		}
		cfg := c.view.GetVersionedRulesConfig()
		if cfg != nil {
			cfgs[user] = *cfg
		}
	}
	return cfgs, nil
}
