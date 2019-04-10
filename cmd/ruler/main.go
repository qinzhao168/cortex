package main

import (
	"flag"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"
)

func main() {
	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
		}
		ringConfig        ring.Config
		distributorConfig distributor.Config
		clientConfig      client.Config
		limits            validation.Limits

		rulerConfig       ruler.Config
		configStoreConfig ruler.ConfigStoreConfig
		chunkStoreConfig  chunk.StoreConfig
		schemaConfig      chunk.SchemaConfig
		storageConfig     storage.Config
		querierConfig     querier.Config
	)

	// Ruler Lifecycler needs to know our gRPC listen port.
	rulerConfig.LifecyclerConfig.ListenPort = &serverConfig.GRPCListenPort

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("ruler")
	defer trace.Close()

	flagext.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig, &clientConfig, &limits,
		&rulerConfig, &configStoreConfig, &chunkStoreConfig, &storageConfig, &schemaConfig, &querierConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	overrides, err := validation.NewOverrides(limits)
	util.CheckFatal("initializing overrides", err)
	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides)
	util.CheckFatal("", err)
	defer chunkStore.Stop()

	r, err := ring.New(ringConfig, "ingester")
	util.CheckFatal("initializing ring", err)

	prometheus.MustRegister(r)
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, clientConfig, overrides, r)
	util.CheckFatal("initializing distributor", err)
	defer dist.Stop()

	rulesAPI, err := ruler.NewRulesAPI(configStoreConfig)
	util.CheckFatal("error initializing rules API", err)

	querierConfig.MaxConcurrent = rulerConfig.NumWorkers
	querierConfig.Timeout = rulerConfig.GroupTimeout
	queryable, engine := querier.New(querierConfig, dist, chunkStore)

	rlr, err := ruler.NewRuler(rulerConfig, engine, queryable, dist, rulesAPI)
	util.CheckFatal("error initializing ruler", err)
	defer rlr.Stop()

	server, err := server.New(serverConfig)
	util.CheckFatal("initializing server", err)
	defer server.Shutdown()

	// Only serve the API for setting & getting rules configs if we're not
	// serving configs from the configs API. Allows for smoother
	// migration. See https://github.com/cortexproject/cortex/issues/619
	if configStoreConfig.ConfigsAPIURL.URL == nil {
		a, err := ruler.NewAPIFromConfig(configStoreConfig.DBConfig)
		util.CheckFatal("initializing public rules API", err)

		a.RegisterRoutes(server.HTTP)
	}

	server.HTTP.Handle("/ring", r)
	server.HTTP.Handle("/ruler_ring", rlr)
	server.Run()
}
