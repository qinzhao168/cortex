package generator

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	promStorage "github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// Config is the configuration for the recording rules server.
type Config struct {
	EvaluationInterval time.Duration `yaml:"evaluation_interval"`
	JobsFile           string        `yaml:"jobs_file"`
}

type JobsConfig struct {
	Jobs []Job `yaml:"jobs"`
}

type Job struct {
	From  int64  `yaml:"from"`
	To    int64  `yaml:"to"`
	Query string `yaml:"query"`
	Name  string `yaml:"name"`
	User  string `yaml:"user"`
}

func (j Job) toString() string {
	return fmt.Sprintf("{from=%v, to=%v, query='%v', name='%v', user=%v", j.From, j.To, j.Query, j.Name, j.User)
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.EvaluationInterval, "generator.evaluation-interval", 1*time.Minute, "Interval at which to evaluate a series.")
	f.StringVar(&cfg.JobsFile, "generator.jobs-file", "", "Yaml file containing description of generator jobs")
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

	if g.cfg.JobsFile == "" {
		return errors.New("no jobs file specified")
	}

	f, err := os.Open(g.cfg.JobsFile)
	if err != nil {
		return err
	}

	jobs := JobsConfig{}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)
	err = decoder.Decode(&jobs)
	if err != nil {
		return err
	}

	for _, job := range jobs.Jobs {
		level.Info(g.logger).Log("msg", "running job", "job", job.toString())
		err = g.executeJob(ctx, job)
		if err != nil {
			return err
		}
	}

	return util.ErrStopProcess
}

func (g *Generator) executeJob(ctx context.Context, job Job) error {
	ctx = user.InjectOrgID(ctx, job.User)

	expr, err := promql.ParseExpr(job.Query)
	if err != nil {
		level.Error(g.logger).Log("msg", "unable to parse promql expression", "err", err)
		return err
	}

	cur := model.TimeFromUnix(job.From)
	end := model.TimeFromUnix(job.To)

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
			lb.Set(labels.MetricName, job.Name)
			sample.Metric = lb.Labels()

			fp := client.Fingerprint(sample.Metric)
			if err != nil {
				return err
			}

			series, exists := memSeriesMap.Get(fp)
			if !exists {
				series = ingester.NewMemorySeries(sample.Metric, prometheus.NewCounter(prometheus.CounterOpts{
					Name: "dummy_counter",
				}))
				memSeriesMap.Put(fp, series)
			}

			err = series.Add(model.SamplePair{
				Timestamp: model.Time(sample.T),
				Value:     model.SampleValue(sample.V),
			})

			if err != nil {
				level.Error(g.logger).Log("msg", "unable to add sample pair", "err", err)
				return err
			}
		}

		cur = cur.Add(g.cfg.EvaluationInterval)
	}

	for fpp := range memSeriesMap.Iter() {
		fpp.Series.CloseHead(1)
		err = g.flushChunks(ctx, job.User, fpp.Fp, fpp.Series.Metric, fpp.Series.ChunkDescs)
		if err != nil {
			return err
		}
	}

	return nil
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
