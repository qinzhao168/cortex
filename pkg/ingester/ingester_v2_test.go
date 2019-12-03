package ingester

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

func TestIngester_v2Query_ShouldNotCreateTSDBIfDoesNotExist(t *testing.T) {
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig())
	require.NoError(t, err)
	defer i.Shutdown()
	defer cleanup()

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.QueryRequest{}

	res, err := i.v2Query(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.QueryResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_v2LabelValues_ShouldNotCreateTSDBIfDoesNotExist(t *testing.T) {
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig())
	require.NoError(t, err)
	defer i.Shutdown()
	defer cleanup()

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelValuesRequest{}

	res, err := i.v2LabelValues(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelValuesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_v2LabelNames_ShouldNotCreateTSDBIfDoesNotExist(t *testing.T) {
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig())
	require.NoError(t, err)
	defer i.Shutdown()
	defer cleanup()

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelNamesRequest{}

	res, err := i.v2LabelNames(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelNamesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_v2Push_ShouldNotCreateTSDBIfNotInActiveState(t *testing.T) {
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig())
	require.NoError(t, err)
	defer i.Shutdown()
	defer cleanup()
	require.Equal(t, ring.PENDING, i.lifecycler.GetState())

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.WriteRequest{}

	res, err := i.v2Push(ctx, req)
	assert.Equal(t, fmt.Errorf(errTSDBCreateIncompatibleState, "PENDING"), err)
	assert.Nil(t, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_getOrCreateTSDB_ShouldNotAllowToCreateTSDBIfIngesterStateIsNotActive(t *testing.T) {
	tests := map[string]struct {
		state       ring.IngesterState
		expectedErr error
	}{
		"not allow to create TSDB if in PENDING state": {
			state:       ring.PENDING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.PENDING),
		},
		"not allow to create TSDB if in JOINING state": {
			state:       ring.JOINING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.JOINING),
		},
		"not allow to create TSDB if in LEAVING state": {
			state:       ring.LEAVING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.LEAVING),
		},
		"allow to create TSDB if in ACTIVE state": {
			state:       ring.ACTIVE,
			expectedErr: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := defaultIngesterTestConfig()
			cfg.LifecyclerConfig.JoinAfter = 60 * time.Second

			i, cleanup, err := newIngesterMockWithTSDBStorage(cfg)
			require.NoError(t, err)
			defer i.Shutdown()
			defer cleanup()
			defer os.RemoveAll(i.cfg.TSDBConfig.Dir)

			// Switch ingester state to the expected one in the test
			if i.lifecycler.GetState() != testData.state {
				var stateChain []ring.IngesterState

				if testData.state == ring.LEAVING {
					stateChain = []ring.IngesterState{ring.ACTIVE, ring.LEAVING}
				} else {
					stateChain = []ring.IngesterState{testData.state}
				}

				for _, s := range stateChain {
					err = i.lifecycler.ChangeState(context.Background(), s)
					require.NoError(t, err)
				}
			}

			db, err := i.getOrCreateTSDB("test", false)
			assert.Equal(t, testData.expectedErr, err)

			if testData.expectedErr != nil {
				assert.Nil(t, db)
			} else {
				assert.NotNil(t, db)
			}
		})
	}
}

func Test_Ingester_v2MetricsForLabelMatchers(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "l"}, {Name: "uniq0", Value: "0"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "m"}, {Name: "uniq0", Value: "1"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
	}

	tests := map[string]struct {
		from     int64
		to       int64
		matchers []*client.LabelMatchers
		expected []*client.Metric
	}{
		"should return an empty response if no metric match": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
				},
			}},
			expected: []*client.Metric{},
		},
		"should filter metrics by single matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[1].lbls)},
			},
		},
		"should filter metrics by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: "status", Value: "200"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_2"},
					},
				},
			},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should filter metrics by time range": {
			from: 100000,
			to:   100000,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[0].lbls)},
			},
		},
		"should not return duplicated metrics on overlapping matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
					},
				},
			},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[1].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "collision"},
				},
			}},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[3].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[4].lbls)},
			},
		},
	}

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig())
	require.NoError(t, err)
	defer i.Shutdown()
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push fixtures
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range fixtures {
		req, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			req := &client.MetricsForLabelMatchersRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				MatchersSet:      testData.matchers,
			}

			res, err := i.v2MetricsForLabelMatchers(ctx, req)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res.Metric)
		})
	}
}

func mockWriteRequest(lbls labels.Labels, value float64, timestampMs int64) (*client.WriteRequest, *client.QueryResponse) {
	samples := []client.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	req := client.ToWriteRequest([]labels.Labels{lbls}, samples, client.API)

	// Generate the expected response
	expectedResponse := &client.QueryResponse{
		Timeseries: []client.TimeSeries{
			{
				Labels:  client.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	return req, expectedResponse
}

func newIngesterMockWithTSDBStorage(ingesterCfg Config) (*Ingester, func(), error) {
	clientCfg := defaultClientTestConfig()
	limits := defaultLimitsTestConfig()

	overrides, err := validation.NewOverrides(limits, nil)
	if err != nil {
		return nil, nil, err
	}

	// Create a temporary directory for TSDB
	tempDir, err := ioutil.TempDir("", "tsdb")
	if err != nil {
		return nil, nil, err
	}

	ingesterCfg.TSDBEnabled = true
	ingesterCfg.TSDBConfig.Dir = tempDir
	ingesterCfg.TSDBConfig.Backend = "s3"
	ingesterCfg.TSDBConfig.S3.Endpoint = "localhost"

	ingester, err := NewV2(ingesterCfg, clientCfg, overrides, nil)
	if err != nil {
		return nil, nil, err
	}

	// Create a cleanup function that the caller should call with defer
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return ingester, cleanup, nil
}
