package testutils

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/util/flagext"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
)

// Fixture type for per-backend testing.
type Fixture interface {
	Name() string
	Clients() (chunk.IndexClient, chunk.Client, chunk.TableClient, chunk.SchemaConfig, error)
	Teardown() error
}

// DefaultSchemaConfig returns default schema for use in test fixtures
func DefaultSchemaConfig(kind string) chunk.SchemaConfig {
	schemaConfig := chunk.DefaultSchemaConfig(kind, "v1", model.Now().Add(-time.Hour*2))
	return schemaConfig
}

// Setup a fixture with initial tables
func Setup(fixture Fixture, tableName string) (chunk.IndexClient, chunk.Client, error) {
	var tbmConfig chunk.TableManagerConfig
	flagext.DefaultValues(&tbmConfig)
	indexClient, chunkClient, tableClient, schemaConfig, err := fixture.Clients()
	if err != nil {
		return nil, nil, err
	}

	tableManager, err := chunk.NewTableManager(tbmConfig, schemaConfig, 12*time.Hour, tableClient, nil)
	if err != nil {
		return nil, nil, err
	}

	err = tableManager.SyncTables(context.Background())
	if err != nil {
		return nil, nil, err
	}

	err = tableClient.CreateTable(context.Background(), chunk.TableDesc{
		Name: tableName,
	})

	return indexClient, chunkClient, err
}

// CreateChunks creates some chunks for testing
func CreateChunks(startIndex, batchSize int, options ...CreateChunkOptions) ([]string, []chunk.Chunk, error) {
	req := &createChunkRequest{
		userID: "userID",
		from:   model.Now(),
	}
	for _, opt := range options {
		opt.set(req)
	}
	keys := []string{}
	chunks := []chunk.Chunk{}
	for j := 0; j < batchSize; j++ {
		chunk := dummyChunkFor(req.from, labels.Labels{
			{Name: model.MetricNameLabel, Value: "foo"},
			{Name: "index", Value: strconv.Itoa(startIndex*batchSize + j)},
		}, req.userID)
		chunks = append(chunks, chunk)
		keys = append(keys, chunk.ExternalKey())
	}
	return keys, chunks, nil
}

func dummyChunkFor(now model.Time, metric labels.Labels, userID string) chunk.Chunk {
	cs := promchunk.New()
	cs.Add(model.SamplePair{Timestamp: now, Value: 0})
	chunk := chunk.NewChunk(
		userID,
		client.Fingerprint(metric),
		metric,
		cs,
		now.Add(-time.Hour),
		now,
	)
	// Force checksum calculation.
	err := chunk.Encode()
	if err != nil {
		panic(err)
	}
	return chunk
}

// CreateChunkOptions allows options to be passed when creating chunks
type CreateChunkOptions interface {
	set(req *createChunkRequest)
}

type createChunkRequest struct {
	userID string
	from   model.Time
}

// From applies a new user ID to chunks created by CreateChunks
func From(from model.Time) FromOpt { return FromOpt(from) }

// FromOpt is used to set the from field of the chunks
type FromOpt model.Time

func (f FromOpt) set(req *createChunkRequest) {
	req.from = model.Time(f)
}

// User applies a new user ID to chunks created by CreateChunks
func User(user string) UserOpt { return UserOpt(user) }

// UserOpt is used to set the user for a set of chunks
type UserOpt string

func (u UserOpt) set(req *createChunkRequest) {
	req.userID = string(u)
}
