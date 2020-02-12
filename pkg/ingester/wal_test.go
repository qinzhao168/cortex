package ingester

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestWAL(t *testing.T) {
	dirname, err := ioutil.TempDir("", "cortex-wal")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dirname))
	}()

	cfg := defaultIngesterTestConfig()
	cfg.WALConfig.WALEnabled = true
	cfg.WALConfig.CheckpointEnabled = true
	cfg.WALConfig.Recover = true
	cfg.WALConfig.Dir = dirname
	cfg.WALConfig.CheckpointDuration = 100 * time.Millisecond

	numSeries := 100
	numSamplesPerSeriesPerPush := 10
	numRestarts := 3

	// Build an ingester, add some samples, then shut it down.
	_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
	userIDs, testData := pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, 0)
	ing.Shutdown()

	for r := 0; r < numRestarts; r++ {
		if r == numRestarts-1 {
			cfg.WALConfig.WALEnabled = false
			cfg.WALConfig.CheckpointEnabled = false
		}
		// Start a new ingester and recover the WAL.
		_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())

		for i, userID := range userIDs {
			testData[userID] = buildTestMatrix(numSeries, (r+1)*numSamplesPerSeriesPerPush, i)
		}
		// Check the samples are still there!
		retrieveTestSamples(t, ing, userIDs, testData)

		if r != numRestarts-1 {
			userIDs, testData = pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, (r+1)*numSamplesPerSeriesPerPush)
		}

		ing.Shutdown()
	}
}

func TestCheckpointRepair(t *testing.T) {
	dirname, err := ioutil.TempDir("", "cortex-wal")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dirname))
	}()

	cfg := defaultIngesterTestConfig()
	cfg.WALConfig.WALEnabled = true
	cfg.WALConfig.CheckpointEnabled = true
	cfg.WALConfig.Recover = true
	cfg.WALConfig.Dir = dirname
	cfg.WALConfig.CheckpointDuration = 100 * time.Hour // Basically no automatic checkpoint.

	numSeries := 100
	numSamplesPerSeriesPerPush := 10

	// Build an ingester, add some samples, then shut it down.
	_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())

	w, ok := ing.wal.(*walWrapper)
	require.True(t, ok)

	// First checkpoint.
	userIDs, testData := pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, 0)
	require.NoError(t, w.performCheckpoint())

	// More samples for the 2nd checkpoint. 2nd checkpoint is created during shutdown.
	_, _ = pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, numSamplesPerSeriesPerPush)
	ing.Shutdown()

	require.Equal(t, 2.0, prom_testutil.ToFloat64(w.checkpointCreationTotal))

	// Verify 2 checkpoint dirs.
	files, err := ioutil.ReadDir(w.wal.Dir())
	require.NoError(t, err)
	numDirs := 0
	for _, f := range files {
		if f.IsDir() {
			numDirs++
		}
	}
	require.Equal(t, 2, numDirs)

	// Corrupt the last checkpoint.
	lastChDir, _, err := lastCheckpoint(w.wal.Dir())
	require.NoError(t, err)
	files, err = ioutil.ReadDir(lastChDir)
	require.NoError(t, err)

	lastFile, err := os.OpenFile(filepath.Join(lastChDir, files[len(files)-1].Name()), os.O_WRONLY, os.ModeAppend)
	require.NoError(t, err)
	n, err := lastFile.WriteAt([]byte{1, 2, 3, 4}, 2)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.NoError(t, lastFile.Close())

	// Open an ingester for the repair.
	_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
	w, ok = ing.wal.(*walWrapper)
	require.True(t, ok)
	defer ing.Shutdown()

	require.Equal(t, 1.0, prom_testutil.ToFloat64(ing.metrics.walCorruptionsTotal))

	// Verify 1 checkpoint dirs after the corrupt checkpoint is deleted.
	files, err = ioutil.ReadDir(w.wal.Dir())
	require.NoError(t, err)
	numDirs = 0
	for _, f := range files {
		if f.IsDir() {
			numDirs++
		}
	}
	require.Equal(t, 1, numDirs)

	// Verify we did not lose any data.
	for i, userID := range userIDs {
		// '2*' because we ingested the data twice.
		testData[userID] = buildTestMatrix(numSeries, 2*numSamplesPerSeriesPerPush, i)
	}
	retrieveTestSamples(t, ing, userIDs, testData)
}
