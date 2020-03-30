package generator

import (
	"context"
	"errors"
	"flag"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	promStorage "github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// Config is the configuration for the recording rules server.
type Config struct {
	EvaluationInterval time.Duration `yaml:"evaluation_interval"`
	From               int64         `yaml:"from"`
	To                 int64         `yaml:"to"`
	Query              string        `yaml:"query"`
	Name               string        `yaml:"name"`
	User               string        `yaml:"user"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.EvaluationInterval, "generator.evaluation-interval", 1*time.Minute, "Interval at which to evaluate a series.")
	f.Int64Var(&cfg.From, "generator.from", -1, "First timestamp to evaluate the provided query.")
	f.Int64Var(&cfg.To, "generator.to", -1, "Last timestamp to evaluate the provided query.")
	f.StringVar(&cfg.Query, "generator.query", "", "Query to evaluate and write to configured chunk store.")
	f.StringVar(&cfg.Name, "generator.name", "", "Query to evaluate and write to configured chunk store.")
	f.StringVar(&cfg.User, "generator.user", "", "Cortex user to query chunks from.")
}

// Generator generates and store a Prometheus series from a queryable
type Generator struct {
	services.Service

	cfg       Config
	engine    *promql.Engine
	queryable promStorage.Queryable
	store     chunk.Store

	registry prometheus.Registerer
	logger   log.Logger
}

// New returns a new Generator
func New(cfg Config, engine *promql.Engine, queryable promStorage.Queryable, store chunk.Store, reg prometheus.Registerer, logger log.Logger) (*Generator, error) {
	generator := &Generator{
		cfg:       cfg,
		engine:    engine,
		queryable: queryable,
		store:     store,
		registry:  reg,
		logger:    logger,
	}

	generator.Service = services.NewBasicService(generator.starting, generator.run, generator.stopping)
	return generator, nil
}

func (g *Generator) starting(ctx context.Context) error {
	return nil
}

func (g *Generator) stopping(_ error) error {
	return nil
}

func (g *Generator) run(ctx context.Context) error {
	level.Info(g.logger).Log("msg", "generator up and running")
	ctx = user.InjectOrgID(ctx, g.cfg.User)

	expr, err := promql.ParseExpr(g.cfg.Query)
	if err != nil {
		level.Error(g.logger).Log("msg", "unable to parse promql expression", "err", err)
		return err
	}

	cur := model.TimeFromUnix(g.cfg.From)
	end := model.TimeFromUnix(g.cfg.To)

	memSeriesMap := ingester.NewSeriesMap()

	for cur.Before(end) {
		q, err := g.engine.NewInstantQuery(g.queryable, expr.String(), cur.Time())
		if err != nil {
			level.Error(g.logger).Log("msg", "unable to generate instant query", "err", err)
			return err
		}

		res := q.Exec(ctx)

		var vector promql.Vector
		switch v := res.Value.(type) {
		case promql.Vector:
			vector = v
		case promql.Scalar:
			vector = promql.Vector{promql.Sample{
				Point:  promql.Point(v),
				Metric: labels.Labels{},
			}}
		default:
			return errors.New("rule result is not a vector or scalar")
		}

		// Override the metric name and labels.
		for i := range vector {
			sample := &vector[i]
			lb := labels.NewBuilder(sample.Metric)
			lb.Set(labels.MetricName, g.cfg.Name)
			sample.Metric = lb.Labels()

			fp, err := model.FingerprintFromString(sample.Metric.String())
			if err != nil {
				return err
			}

			s, exists := memSeriesMap.Get(fp)
			if !exists {
				s = ingester.NewMemorySeries(sample.Metric, nil)
				memSeriesMap.Put(fp, s)
			}

			err = s.Add(model.SamplePair{
				Timestamp: model.Time(sample.T),
				Value:     model.SampleValue(sample.V),
			})

			if err != nil {
				level.Error(g.logger).Log("msg", "unable to add sample pair", "err", err)
				return err
			}
		}
	}

	for fpp := range memSeriesMap.Iter() {
		fpp.Series.CloseHead(1)
		err = g.flushChunks(ctx, g.cfg.User, fpp.Fp, fpp.Series.Metric, fpp.Series.ChunkDescs)
		if err != nil {
			return err
		}
	}

	return util.ErrStopProcess
}

func (g *Generator) flushChunks(ctx context.Context, userID string, fp model.Fingerprint, metric labels.Labels, chunkDescs []*ingester.ChunkDesc) error {
	wireChunks := make([]chunk.Chunk, 0, len(chunkDescs))
	for _, chunkDesc := range chunkDescs {
		c := chunk.NewChunk(userID, fp, metric, chunkDesc.C, chunkDesc.FirstTime, chunkDesc.LastTime)
		if err := c.Encode(); err != nil {
			level.Error(g.logger).Log("msg", "unable to encode chunk", "err", err)
			return err
		}
		wireChunks = append(wireChunks, c)
	}

	if err := g.store.Put(ctx, wireChunks); err != nil {
		level.Error(g.logger).Log("msg", "unable to store chunk", "err", err)
		return err
	}

	return nil
}
