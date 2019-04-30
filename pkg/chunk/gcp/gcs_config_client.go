package gcp

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/pkg/errors"
)

const (
	rulePrefix = "rules/"
)

type gcsConfigsClient struct {
	cfg GCSConfigDBClientConfig

	client *storage.Client
	bucket *storage.BucketHandle
}

// GCSConfigDBClientConfig is config for the GCS Chunk Client.
type GCSConfigDBClientConfig struct {
	GCSConfig GCSConfig
}

// RegisterFlags registers flags.
func (cfg *GCSConfigDBClientConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.GCSConfig.RegisterFlagsWithPrefix("configdb.", f)
}

// NewGCSConfigsClient makes a new chunk.ObjectClient that writes chunks to GCS.
func NewGCSConfigsClient(ctx context.Context, cfg GCSConfigDBClientConfig) (db.DB, error) {
	option, err := gcsInstrumentation(ctx, "configs")
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(ctx, option)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(cfg.GCSConfig.BucketName)
	return &gcsConfigsClient{
		cfg:    cfg,
		client: client,
		bucket: bucket,
	}, nil
}

func (g *gcsConfigsClient) GetRulesConfig(userID string) (configs.VersionedRulesConfig, error) {
	return g.getRulesConfig(rulePrefix + userID)
}

func (g *gcsConfigsClient) getRulesConfig(ruleObj string) (configs.VersionedRulesConfig, error) {
	reader, err := g.bucket.Object(ruleObj).NewReader(context.Background())
	if err != nil {
		return configs.VersionedRulesConfig{}, err
	}
	defer reader.Close()

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return configs.VersionedRulesConfig{}, err
	}

	config := configs.VersionedRulesConfig{}
	err = json.Unmarshal(buf, &config)
	if err != nil {
		return configs.VersionedRulesConfig{}, err
	}

	config.ID = configs.ID(reader.Attrs.Generation)
	return config, nil
}

func (g *gcsConfigsClient) SetRulesConfig(userID string, oldConfig configs.RulesConfig, newConfig configs.RulesConfig) (bool, error) {
	current, err := g.GetRulesConfig(userID)
	if err != nil {
		return false, err
	}

	// The supplied oldConfig must match the current config. If no config
	// exists, then oldConfig must be nil. Otherwise, it must exactly
	// equal the existing config.
	if oldConfig.Equal(current.Config) {
		return false, errors.New("old config provided does not match what is currently stored")
	}

	cfgBytes, err := json.Marshal(newConfig)
	if err != nil {
		return false, err
	}

	writer := g.bucket.Object(rulePrefix + userID).If(storage.Conditions{GenerationMatch: int64(current.ID)}).NewWriter(context.Background())
	if _, err := writer.Write(cfgBytes); err != nil {
		return false, err
	}
	if err := writer.Close(); err != nil {
		return true, err
	}

	return true, nil
}

func (g *gcsConfigsClient) GetAllRulesConfigs() (map[string]configs.VersionedRulesConfig, error) {
	objs := g.bucket.Objects(context.Background(), &storage.Query{
		Prefix: rulePrefix,
	})

	ruleMap := map[string]configs.VersionedRulesConfig{}
	for {
		objAttrs, err := objs.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		rls, err := g.getRulesConfig(objAttrs.Name)
		if err != nil {
			return nil, err
		}
		ruleMap[strings.TrimPrefix(objAttrs.Name, rulePrefix)] = rls
	}

	return ruleMap, nil
}

func (g *gcsConfigsClient) GetRulesConfigs(since time.Time) (map[string]configs.VersionedRulesConfig, error) {
	objs := g.bucket.Objects(context.Background(), &storage.Query{
		Prefix: rulePrefix,
	})

	ruleMap := map[string]configs.VersionedRulesConfig{}
	for {
		objAttrs, err := objs.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		if objAttrs.Updated.After(since) {
			rls, err := g.getRulesConfig(objAttrs.Name)
			if err != nil {
				return nil, err
			}
			ruleMap[strings.TrimPrefix(objAttrs.Name, rulePrefix)] = rls
		}
	}
	return ruleMap, nil
}

func (g *gcsConfigsClient) GetConfig(userID string) (configs.View, error) {
	panic("not implemented")
}

func (g *gcsConfigsClient) SetConfig(userID string, cfg configs.Config) error {
	panic("not implemented")
}

func (g *gcsConfigsClient) GetAllConfigs() (map[string]configs.View, error) {
	panic("not implemented")
}

func (g *gcsConfigsClient) GetConfigs(since time.Time) (map[string]configs.View, error) {
	panic("not implemented")
}

func (g *gcsConfigsClient) DeactivateConfig(userID string) error {
	panic("not implemented")
}

func (g *gcsConfigsClient) RestoreConfig(userID string) error {
	panic("not implemented")
}

func (g *gcsConfigsClient) Close() error {
	panic("not implemented")
}
