package main

import (
	"context"
	"flag"
	"log"
	"strings"

	_ "google.golang.org/grpc/encoding/gzip" // get gzip compressor registered

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func main() {
	var (
		storageConfig storage.Config
		schemaConfig  chunk.SchemaConfig

		table string
		user  string
	)

	flag.StringVar(&table, "table", "", "table to delete data from")
	flag.StringVar(&user, "user", "", "user to delete from table")
	flagext.RegisterFlags(&storageConfig, &schemaConfig)
	flag.Parse()

	if user == "" || strings.Contains(user, ":") {
		log.Fatalf("user '%v' is not valid", user)
	}

	indexClient, err := storage.NewIndexClient("gcp", storageConfig, schemaConfig)
	if err != nil {
		log.Fatalln(err)
	}

	query := chunk.DeleteQuery{
		TableName: table,
		UserID:    user,
	}

	err = indexClient.DeletePages(context.Background(), query)
	if err != nil {
		log.Fatalln(err)
	}
}
