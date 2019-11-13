package ingester

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

func TestIngester_v2Query_ShouldNotCreateTSDBIfDoesNotExist(t *testing.T) {
	i, err := newIngesterMockWithTSDBStorage()
	require.NoError(t, err)
	defer i.Shutdown()

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
	i, err := newIngesterMockWithTSDBStorage()
	require.NoError(t, err)
	defer i.Shutdown()

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
	i, err := newIngesterMockWithTSDBStorage()
	require.NoError(t, err)
	defer i.Shutdown()

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
		testData := testData

		t.Run(testName, func(t *testing.T) {
			i, err := newIngesterMockWithTSDBStorage()
			require.NoError(t, err)
			defer i.Shutdown()
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

			db, err := i.getOrCreateTSDB("test")
			assert.Equal(t, testData.expectedErr, err)

			if testData.expectedErr != nil {
				assert.Nil(t, db)
			} else {
				assert.NotNil(t, db)
			}
		})
	}
}

func newIngesterMockWithTSDBStorage() (*Ingester, error) {
	ingesterCfg := defaultIngesterTestConfig()
	clientCfg := defaultClientTestConfig()
	limits := defaultLimitsTestConfig()

	overrides, err := validation.NewOverrides(limits)
	if err != nil {
		return nil, err
	}

	// Create a temporary directory for TSDB
	tempDir, err := ioutil.TempDir("", "tsdb")
	if err != nil {
		return nil, err
	}

	ingesterCfg.TSDBEnabled = true
	ingesterCfg.TSDBConfig.Dir = tempDir
	ingesterCfg.TSDBConfig.Backend = "s3"
	ingesterCfg.TSDBConfig.S3.Endpoint = "localhost"

	return NewV2(ingesterCfg, clientCfg, overrides, nil)
}
