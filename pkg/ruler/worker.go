package ruler

import (
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

// Worker does a thing until it's told to stop.
type Worker interface {
	Run()
	Stop()
}

type worker struct {
	scheduler *scheduler
	ruler     *Ruler

	quit chan struct{}
	done chan struct{}
}

func newWorker(ruler *Ruler) worker {
	return worker{
		scheduler: ruler.scheduler,
		ruler:     ruler,
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (w *worker) Run() {
	defer close(w.done)
	for {
		select {
		case <-w.quit:
			return
		default:
		}
		waitStart := time.Now()
		blockedWorkers.Inc()
		level.Debug(util.Logger).Log("msg", "waiting for next work item")
		item := w.scheduler.nextWorkItem()
		blockedWorkers.Dec()
		waitElapsed := time.Now().Sub(waitStart)
		if item == nil {
			level.Debug(util.Logger).Log("msg", "queue closed and empty; terminating worker")
			return
		}
		evalLatency.Observe(time.Since(item.scheduled).Seconds())
		workerIdleTime.Add(waitElapsed.Seconds())
		level.Debug(util.Logger).Log("msg", "processing item", "item", item)
		w.ruler.Evaluate(item.userID, item)
		w.scheduler.workItemDone(*item)
		level.Debug(util.Logger).Log("msg", "item handed back to queue", "item", item)
	}
}

func (w *worker) Stop() {
	close(w.quit)
	<-w.done
}
