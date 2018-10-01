package chunk

import (
	"context"
	"sync"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/weaveworks/cortex/pkg/chunk/cache"
	"github.com/weaveworks/cortex/pkg/util"
	"github.com/weaveworks/cortex/pkg/util/spanlogger"
)

const chunkDecodeParallelism = 16

func filterChunksByTime(from, through model.Time, chunks []Chunk) ([]Chunk, []string) {
	filtered := make([]Chunk, 0, len(chunks))
	keys := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.Through < from || through < chunk.From {
			continue
		}
		filtered = append(filtered, chunk)
		keys = append(keys, chunk.ExternalKey())
	}
	return filtered, keys
}

func filterChunksByMatchers(chunks []Chunk, filters []*labels.Matcher) []Chunk {
	filteredChunks := make([]Chunk, 0, len(chunks))
outer:
	for _, chunk := range chunks {
		for _, filter := range filters {
			if !filter.Matches(string(chunk.Metric[model.LabelName(filter.Name)])) {
				continue outer
			}
		}
		filteredChunks = append(filteredChunks, chunk)
	}
	return filteredChunks
}

// Fetcher deals with fetching chunk contents from the cache/store,
// and writing back any misses to the cache.  Also responsible for decoding
// chunks from the cache, in parallel.
type Fetcher struct {
	storage StorageClient
	cache   cache.Cache

	wait           sync.WaitGroup
	decodeRequests chan decodeRequest
}

type decodeRequest struct {
	chunk     Chunk
	buf       []byte
	responses chan decodeResponse
}
type decodeResponse struct {
	chunk Chunk
	err   error
}

// NewChunkFetcher makes a new ChunkFetcher.
func NewChunkFetcher(cfg cache.Config, storage StorageClient) (*Fetcher, error) {
	cache, err := cache.New(cfg)
	if err != nil {
		return nil, err
	}

	c := &Fetcher{
		storage:        storage,
		cache:          cache,
		decodeRequests: make(chan decodeRequest),
	}

	c.wait.Add(chunkDecodeParallelism)
	for i := 0; i < chunkDecodeParallelism; i++ {
		go c.worker()
	}

	return c, nil
}

// Stop the ChunkFetcher.
func (c *Fetcher) Stop() {
	close(c.decodeRequests)
	c.wait.Wait()
	c.cache.Stop()
}

func (c *Fetcher) worker() {
	defer c.wait.Done()
	decodeContext := NewDecodeContext()
	for req := range c.decodeRequests {
		err := req.chunk.Decode(decodeContext, req.buf)
		if err != nil {
			cacheCorrupt.Inc()
		}
		req.responses <- decodeResponse{
			chunk: req.chunk,
			err:   err,
		}
	}
}

// FetchChunks fetchers a set of chunks from cache and store.
func (c *Fetcher) FetchChunks(ctx context.Context, chunks []Chunk, keys []string) ([]Chunk, error) {
	log, ctx := spanlogger.New(ctx, "ChunkStore.fetchChunks")
	defer log.Span.Finish()

	// Now fetch the actual chunk data from Memcache / S3
	cacheHits, cacheBufs, _ := c.cache.Fetch(ctx, keys)

	fromCache, missing, err := c.processCacheResponse(chunks, cacheHits, cacheBufs)
	if err != nil {
		level.Warn(log).Log("msg", "error fetching from cache", "err", err)
	}

	var fromStorage []Chunk
	if len(missing) > 0 {
		fromStorage, err = c.storage.GetChunks(ctx, missing)
	}

	// Always cache any chunks we did get
	if cacheErr := c.writeBackCache(ctx, fromStorage); cacheErr != nil {
		level.Warn(log).Log("msg", "could not store chunks in chunk cache", "err", cacheErr)
	}

	if err != nil {
		return nil, promql.ErrStorage(err)
	}

	allChunks := append(fromCache, fromStorage...)
	return allChunks, nil
}

func (c *Fetcher) writeBackCache(ctx context.Context, chunks []Chunk) error {
	keys := make([]string, 0, len(chunks))
	bufs := make([][]byte, 0, len(chunks))
	for i := range chunks {
		encoded, err := chunks[i].Encode()
		// TODO don't fail, just log and conitnue?
		if err != nil {
			return err
		}

		keys = append(keys, chunks[i].ExternalKey())
		bufs = append(bufs, encoded)
	}

	c.cache.Store(ctx, keys, bufs)
	return nil
}

// ProcessCacheResponse decodes the chunks coming back from the cache, separating
// hits and misses.
func (c *Fetcher) processCacheResponse(chunks []Chunk, keys []string, bufs [][]byte) ([]Chunk, []Chunk, error) {
	var (
		requests  = make([]decodeRequest, 0, len(keys))
		responses = make(chan decodeResponse)
		missing   []Chunk
	)

	i, j := 0, 0
	for i < len(chunks) && j < len(keys) {
		chunkKey := chunks[i].ExternalKey()

		if chunkKey < keys[j] {
			missing = append(missing, chunks[i])
			i++
		} else if chunkKey > keys[j] {
			level.Warn(util.Logger).Log("msg", "got chunk from cache we didn't ask for")
			j++
		} else {
			requests = append(requests, decodeRequest{
				chunk:     chunks[i],
				buf:       bufs[j],
				responses: responses,
			})
			i++
			j++
		}
	}
	for ; i < len(chunks); i++ {
		missing = append(missing, chunks[i])
	}

	go func() {
		for _, request := range requests {
			c.decodeRequests <- request
		}
	}()

	var (
		err   error
		found []Chunk
	)
	for i := 0; i < len(requests); i++ {
		response := <-responses

		// Don't exit early, as we don't want to block the workers.
		if response.err != nil {
			err = response.err
		} else {
			found = append(found, response.chunk)
		}
	}
	return found, missing, err
}
