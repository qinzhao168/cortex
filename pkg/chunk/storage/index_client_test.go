package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/stretchr/testify/require"
)

func TestIndexBasic(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client chunk.IndexClient, _ chunk.ObjectClient) {
		// Write out 30 entries, into different hash and range values.
		batch := client.NewWriteBatch()
		for i := 0; i < 30; i++ {
			batch.Add(tableName, fmt.Sprintf("hash%d", i), []byte(fmt.Sprintf("range%d", i)), nil)
		}
		err := client.BatchWrite(context.Background(), batch)
		require.NoError(t, err)

		// Make sure we get back the correct entries by hash value.
		for i := 0; i < 30; i++ {
			entries := []chunk.IndexQuery{
				{
					TableName: tableName,
					HashValue: fmt.Sprintf("hash%d", i),
				},
			}
			var have []chunk.IndexEntry
			err := client.QueryPages(context.Background(), entries, func(_ chunk.IndexQuery, read chunk.ReadBatch) bool {
				iter := read.Iterator()
				for iter.Next() {
					have = append(have, chunk.IndexEntry{
						RangeValue: iter.RangeValue(),
					})
				}
				return true
			})
			require.NoError(t, err)
			require.Equal(t, []chunk.IndexEntry{
				{RangeValue: []byte(fmt.Sprintf("range%d", i))},
			}, have)
		}
	})
}

var entries = []chunk.IndexEntry{
	{
		TableName:  tableName,
		HashValue:  "foo",
		RangeValue: []byte("bar:1"),
		Value:      []byte("10"),
	},
	{
		TableName:  tableName,
		HashValue:  "foo",
		RangeValue: []byte("bar:2"),
		Value:      []byte("20"),
	},
	{
		TableName:  tableName,
		HashValue:  "foo",
		RangeValue: []byte("bar:3"),
		Value:      []byte("30"),
	},
	{
		TableName:  tableName,
		HashValue:  "foo",
		RangeValue: []byte("baz:1"),
		Value:      []byte("10"),
	},
	{
		TableName:  tableName,
		HashValue:  "foo",
		RangeValue: []byte("baz:2"),
		Value:      []byte("20"),
	},
	{
		TableName:  tableName,
		HashValue:  "flip",
		RangeValue: []byte("bar:1"),
		Value:      []byte("abc"),
	},
	{
		TableName:  tableName,
		HashValue:  "flip",
		RangeValue: []byte("bar:2"),
		Value:      []byte("abc"),
	},
	{
		TableName:  tableName,
		HashValue:  "flip",
		RangeValue: []byte("bar:3"),
		Value:      []byte("abc"),
	},
}

func TestQueryPages(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client chunk.IndexClient, _ chunk.ObjectClient) {
		batch := client.NewWriteBatch()
		for _, entry := range entries {
			batch.Add(entry.TableName, entry.HashValue, entry.RangeValue, entry.Value)
		}

		err := client.BatchWrite(context.Background(), batch)
		require.NoError(t, err)

		tests := []struct {
			name   string
			query  chunk.IndexQuery
			repeat bool
			want   []chunk.IndexEntry
		}{
			{
				"check HashValue only",
				chunk.IndexQuery{
					TableName: tableName,
					HashValue: "flip",
				},
				false,
				[]chunk.IndexEntry{entries[5], entries[6], entries[7]},
			},
			{
				"check RangeValueStart",
				chunk.IndexQuery{
					TableName:       tableName,
					HashValue:       "foo",
					RangeValueStart: []byte("bar:2"),
				},
				false,
				[]chunk.IndexEntry{entries[1], entries[2], entries[3], entries[4]},
			},
			{
				"check RangeValuePrefix",
				chunk.IndexQuery{
					TableName:        tableName,
					HashValue:        "foo",
					RangeValuePrefix: []byte("baz:"),
				},
				false,
				[]chunk.IndexEntry{entries[3], entries[4]},
			},
			{
				"check ValueEqual",
				chunk.IndexQuery{
					TableName:        tableName,
					HashValue:        "foo",
					RangeValuePrefix: []byte("bar"),
					ValueEqual:       []byte("20"),
				},
				false,
				[]chunk.IndexEntry{entries[1]},
			},
			{
				"check retry logic",
				chunk.IndexQuery{
					TableName:        tableName,
					HashValue:        "foo",
					RangeValuePrefix: []byte("bar"),
					ValueEqual:       []byte("20"),
				},
				true,
				[]chunk.IndexEntry{entries[1]},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				run := true
				for run {
					var have []chunk.IndexEntry
					err = client.QueryPages(context.Background(), []chunk.IndexQuery{tt.query}, func(_ chunk.IndexQuery, read chunk.ReadBatch) bool {
						iter := read.Iterator()
						for iter.Next() {
							have = append(have, chunk.IndexEntry{
								TableName:  tt.query.TableName,
								HashValue:  tt.query.HashValue,
								RangeValue: iter.RangeValue(),
								Value:      iter.Value(),
							})
						}
						return true
					})
					require.NoError(t, err)
					require.Equal(t, tt.want, have)

					if tt.repeat {
						tt.repeat = false
					} else {
						run = false
					}
				}
			})
		}
	})
}

var deleteEntries = []chunk.IndexEntry{
	{
		TableName:  tableName,
		HashValue:  "1:test1",
		RangeValue: []byte("bar:1"),
		Value:      []byte("10"),
	},
	{
		TableName:  tableName,
		HashValue:  "1:test2",
		RangeValue: []byte("bar:3"),
		Value:      []byte("30"),
	},
	{
		TableName:  tableName,
		HashValue:  "1:test3",
		RangeValue: []byte("baz:1"),
		Value:      []byte("10"),
	},
	{
		TableName:  tableName,
		HashValue:  "2:test1",
		RangeValue: []byte("bar:1"),
		Value:      []byte("abc"),
	},
}

func TestDeletePages(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client chunk.IndexClient, _ chunk.ObjectClient) {
		err := client.DeletePages(context.Background(), chunk.DeleteQuery{UserID: "placeholder"})
		if err == chunk.ErrDeleteNotImplemented {
			return
		}

		batch := client.NewWriteBatch()
		for _, entry := range deleteEntries {
			batch.Add(entry.TableName, entry.HashValue, entry.RangeValue, entry.Value)
		}

		err = client.BatchWrite(context.Background(), batch)
		require.NoError(t, err)

		type testQuery struct {
			query chunk.IndexQuery
			want  []chunk.IndexEntry
		}
		tests := []struct {
			name            string
			query           chunk.DeleteQuery
			preTestQueries  []testQuery
			postTestQueries []testQuery
		}{
			{
				"delete with only hash value",
				chunk.DeleteQuery{
					TableName: tableName,
					HashValue: "1:test1",
				},
				[]testQuery{
					{
						chunk.IndexQuery{
							TableName: tableName,
							HashValue: "1:test1",
						},
						[]chunk.IndexEntry{deleteEntries[0]},
					},
				},
				[]testQuery{
					{
						chunk.IndexQuery{
							TableName: tableName,
							HashValue: "1:test1",
						},
						nil,
					},
				},
			},
			{
				"delete with userID",
				chunk.DeleteQuery{
					TableName: tableName,
					UserID:    "1",
				},
				[]testQuery{
					{
						chunk.IndexQuery{
							TableName: tableName,
							HashValue: "1:test2",
						},
						[]chunk.IndexEntry{deleteEntries[1]},
					},
					{
						chunk.IndexQuery{
							TableName: tableName,
							HashValue: "1:test3",
						},
						[]chunk.IndexEntry{deleteEntries[2]},
					},
				},
				[]testQuery{
					{
						chunk.IndexQuery{
							TableName: tableName,
							HashValue: "1:test2",
						},
						nil,
					},
					{
						chunk.IndexQuery{
							TableName: tableName,
							HashValue: "1:test3",
						},
						nil,
					},
					{
						chunk.IndexQuery{
							TableName: tableName,
							HashValue: "2:test1",
						},
						[]chunk.IndexEntry{deleteEntries[3]},
					},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				for _, tq := range tt.preTestQueries {
					var have []chunk.IndexEntry
					err = client.QueryPages(context.Background(), []chunk.IndexQuery{tq.query}, func(_ chunk.IndexQuery, read chunk.ReadBatch) bool {
						iter := read.Iterator()
						for iter.Next() {
							have = append(have, chunk.IndexEntry{
								TableName:  tq.query.TableName,
								HashValue:  tq.query.HashValue,
								RangeValue: iter.RangeValue(),
								Value:      iter.Value(),
							})
						}
						return true
					})
					require.NoError(t, err)
					require.Equal(t, tq.want, have, "Pre deletion value not as expected")
				}

				err = client.DeletePages(context.Background(), tt.query)
				require.NoError(t, err)

				for _, tq := range tt.postTestQueries {
					var have []chunk.IndexEntry
					err = client.QueryPages(context.Background(), []chunk.IndexQuery{tq.query}, func(_ chunk.IndexQuery, read chunk.ReadBatch) bool {
						iter := read.Iterator()
						for iter.Next() {
							have = append(have, chunk.IndexEntry{
								TableName:  tq.query.TableName,
								HashValue:  tq.query.HashValue,
								RangeValue: iter.RangeValue(),
								Value:      iter.Value(),
							})
						}
						return true
					})
					require.NoError(t, err)
					require.Equal(t, tq.want, have, "Post deletion value not as expected")
				}
			})
		}
	})
}
