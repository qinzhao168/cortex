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
	UserDest           string        `yaml:"user_dest"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.EvaluationInterval, "generator.evaluation-interval", 1*time.Minute, "Interval at which to evaluate a series.")
	f.Int64Var(&cfg.From, "generator.from", -1, "First timestamp to evaluate the provided query.")
	f.Int64Var(&cfg.To, "generator.to", -1, "Last timestamp to evaluate the provided query.")
	f.StringVar(&cfg.Query, "generator.query", "", "Query to evaluate and write to configured chunk store.")
	f.StringVar(&cfg.Name, "generator.name", "", "Query to evaluate and write to configured chunk store.")
	f.StringVar(&cfg.User, "generator.user", "", "Cortex user to query chunks from.")
	f.StringVar(&cfg.UserDest, "generator.dest-user", "", "Cortex user to write chunks for.")
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

// NewGenerator returns a new Generator
func NewGenerator(cfg Config, engine *promql.Engine, queryable promStorage.Queryable, store chunk.Store, reg prometheus.Registerer, logger log.Logger) (*Generator, error) {
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

	c, err := g.queryable.Querier(ctx, g.cfg.From*1000, g.cfg.To*1000)
	if err != nil {
		return err
	}

	expr, err := promql.ParseExpr(g.cfg.Query)
	if err != nil {
		return err
	}

	cur := model.TimeFromUnix(g.cfg.From)
	end := model.TimeFromUnix(g.cfg.To)

	memSeriesMap := ingester.NewSeriesMap()

	for cur.Before(end) {
		q, err := g.engine.NewInstantQuery(v, expr.String(), cur.Time())
		if err != nil {
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

			s.Add(model.SamplePair{
				Timestamp: model.Time(sample.T),
				Value:     model.SampleValue(sample.V),
			})
		}
	}

	return nil
}
