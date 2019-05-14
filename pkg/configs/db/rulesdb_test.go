package db

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/chunk/testutils"
)

const (
	userID    = "userID"
	tableName = "test"
)

type rulesDBTest func(*testing.T, RulesDB)

func forAllFixtures(t *testing.T, test rulesDBTest) {
	var fixtures []testutils.Fixture

	var ruleDB RulesDB
	for _, fixture := range fixtures {
		t.Run(fixture.Name(), func(t *testing.T) {
			test(t, ruleDB)
		})
	}
}

func TestGetConfigsBasic(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, db RulesDB) {
	})
}
