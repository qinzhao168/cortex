package gcp

import (
	"context"
	"flag"
	"io/ioutil"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/cortexproject/cortex/pkg/util"

	gstorage "cloud.google.com/go/storage"
	"github.com/go-kit/kit/log/level"
	"google.golang.org/api/iterator"
)

const (
	alertPrefix = "alerts/"
)

// GCSAlertStoreConfig is config for the GCS Chunk Client.
type GCSAlertStoreConfig struct {
	BucketName string `yaml:"bucket_name"`
}

// RegisterFlags registers flags.
func (cfg *GCSAlertStoreConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, "alertmanager.gcs.bucketname", "", "Name of GCS bucket to put alert configs in.")
}

// GCSAlertStoreClient acts as a config backend. It is not safe to use concurrently when polling for rules.
// This is not an issue with the current scheduler architecture, but must be noted.
type GCSAlertStoreClient struct {
	client *gstorage.Client
	bucket *gstorage.BucketHandle
}

// NewGCSAlertStoreClient makes a new chunk.ObjectClient that writes chunks to GCS.
func NewGCSAlertStoreClient(ctx context.Context, cfg GCSAlertStoreConfig) (*GCSAlertStoreClient, error) {
	client, err := gstorage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return newGCSAlertStoreClient(cfg, client), nil
}

// newGCSAlertStoreClient makes a new chunk.ObjectClient that writes chunks to GCS.
func newGCSAlertStoreClient(cfg GCSAlertStoreConfig, client *gstorage.Client) *GCSAlertStoreClient {
	bucket := client.Bucket(cfg.BucketName)
	return &GCSAlertStoreClient{
		client: client,
		bucket: bucket,
	}
}

// ListAlertConfigs returns all of the active alert configus in this store
func (g *GCSAlertStoreClient) ListAlertConfigs(ctx context.Context) ([]*alerts.AlertConfigDesc, error) {
	it := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: alertPrefix,
	})

	cfgs := []*alerts.AlertConfigDesc{}

	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		alertConfig, err := g.getAlertConfig(ctx, obj.Name)
		if err != nil {
			return nil, err
		}

		cfgs = append(cfgs, alertConfig)
	}

	return cfgs, nil
}

func (g *GCSAlertStoreClient) getAlertConfig(ctx context.Context, obj string) (*alerts.AlertConfigDesc, error) {
	reader, err := g.bucket.Object(obj).NewReader(ctx)
	if err == gstorage.ErrObjectNotExist {
		level.Debug(util.Logger).Log("msg", "object does not exist", "name", obj)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	config := &alerts.AlertConfigDesc{}
	err = config.Unmarshal(buf)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// GetAlertConfig returns a specified users alertmanager configuration
func (g *GCSAlertStoreClient) GetAlertConfig(ctx context.Context, userID string) (*alerts.AlertConfigDesc, error) {
	return g.getAlertConfig(ctx, alertPrefix+userID)
}

// SetAlertConfig sets a specified users alertmanager configuration
func (g *GCSAlertStoreClient) SetAlertConfig(ctx context.Context, cfg *alerts.AlertConfigDesc) error {
	cfgBytes, err := cfg.Marshal()
	if err != nil {
		return err
	}

	objHandle := g.bucket.Object(alertPrefix + cfg.User)

	writer := objHandle.NewWriter(ctx)
	if _, err := writer.Write(cfgBytes); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}

// DeleteAlertConfig deletes a specified users alertmanager configuration
func (g *GCSAlertStoreClient) DeleteAlertConfig(ctx context.Context, userID string) error {
	err := g.bucket.Object(alertPrefix + userID).Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}
