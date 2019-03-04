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
		lifecyclerConfig  ring.LifecyclerConfig
		distributorConfig distributor.Config
		clientConfig      client.Config
		limits            validation.Limits

		rulerConfig      ruler.Config
		ruleStoreConfig  ruler.RuleStoreConfig
		chunkStoreConfig chunk.StoreConfig
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		querierConfig    querier.Config
	)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("ruler")
	defer trace.Close()

	flagext.RegisterFlags(&serverConfig, &lifecyclerConfig, &distributorConfig, &clientConfig, &limits,
		&rulerConfig, &ruleStoreConfig, &chunkStoreConfig, &storageConfig, &schemaConfig, &querierConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	overrides, err := validation.NewOverrides(limits)
	util.CheckFatal("initializing overrides", err)
	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides)
	util.CheckFatal("", err)
	defer chunkStore.Stop()

	r, err := ring.New(lifecyclerConfig.RingConfig)
	util.CheckFatal("initializing ring", err)

	prometheus.MustRegister(r)
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, clientConfig, overrides, r)
	util.CheckFatal("initializing distributor", err)
	defer dist.Stop()

	ruleStore, err := ruler.NewRuleStore(ruleStoreConfig)
	util.CheckFatal("error initializing rules API", err)

	querierConfig.MaxConcurrent = rulerConfig.NumWorkers
	querierConfig.Timeout = rulerConfig.GroupTimeout
	queryable, engine := querier.New(querierConfig, dist, chunkStore)

	rulerConfig.LifecyclerConfig = lifecyclerConfig
	rulerConfig.LifecyclerConfig.ListenPort = &serverConfig.GRPCListenPort
	rulerConfig.LifecyclerConfig.RingConfig = ruler.CreateRulerRingConfig(lifecyclerConfig.RingConfig)
	rlr, err := ruler.NewRuler(rulerConfig, engine, queryable, dist, ruleStore)
	util.CheckFatal("error initializing ruler", err)
	defer rlr.Stop()

	server, err := server.New(serverConfig)
	util.CheckFatal("initializing server", err)
	defer server.Shutdown()

	// Only serve the API for setting & getting rules configs if we're not
	// serving configs from the configs API. Allows for smoother
	// migration. See https://github.com/cortexproject/cortex/issues/619
	if ruleStoreConfig.ConfigsAPIURL.URL == nil {
		a, err := ruler.NewAPIFromConfig(ruleStoreConfig.DBConfig)
		util.CheckFatal("initializing public rules API", err)

		a.RegisterRoutes(server.HTTP)
	}

	server.HTTP.Handle("/ring", r)
	server.HTTP.Handle("/ruler_ring", rlr)
	server.Run()
}
