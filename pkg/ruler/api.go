package ruler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

// RegisterRoutes registers the configs API HTTP routes with the provided Router.
func (r *Ruler) RegisterRoutes(router *mux.Router) {
	if r.store == nil {
		level.Info(util.Logger).Log("msg", "ruler configured with store that does not support api")
		return
	}
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"get_rules", "GET", "/api/prom/api/v1/rules", r.rules},
		{"get_alerts", "GET", "/api/prom/api/v1/rules", r.alerts},
	} {
		level.Debug(util.Logger).Log("msg", "ruler: registering route", "name", route.name, "method", route.method, "path", route.path)
		router.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
}

type errorType string

const (
	errorNone        errorType = ""
	errorTimeout     errorType = "timeout"
	errorCanceled    errorType = "canceled"
	errorExec        errorType = "execution"
	errorBadData     errorType = "bad_data"
	errorInternal    errorType = "internal"
	errorUnavailable errorType = "unavailable"
	errorNotFound    errorType = "not_found"
)

// In order to reimplement the prometheus rules API, a large amount of code was copied over
// This is required because the prometheus api implementation does not pass a context to
// the rule retrieval function.
// https://github.com/prometheus/prometheus/blob/2aacd807b3ec6ddd90ae55f3a42f4cffed561ea9/web/api/v1/api.go#L108
// https://github.com/prometheus/prometheus/pull/4999

type response struct {
	Status    string      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// AlertDiscovery has info for all active alerts.
type AlertDiscovery struct {
	Alerts []*Alert `json:"alerts"`
}

// Alert has info for an alert.
type Alert struct {
	Labels      labels.Labels `json:"labels"`
	Annotations labels.Labels `json:"annotations"`
	State       string        `json:"state"`
	ActiveAt    *time.Time    `json:"activeAt,omitempty"`
	Value       string        `json:"value"`
}

// RuleDiscovery has info for all rules
type RuleDiscovery struct {
	RuleGroups []*RuleGroup `json:"groups"`
}

// RuleGroup has info for rules which are part of a group
type RuleGroup struct {
	Name string `json:"name"`
	File string `json:"file"`
	// In order to preserve rule ordering, while exposing type (alerting or recording)
	// specific properties, both alerting and recording rules are exposed in the
	// same array.
	Rules    []rule  `json:"rules"`
	Interval float64 `json:"interval"`
}

type rule interface{}

type alertingRule struct {
	// State can be "pending", "firing", "inactive".
	State       string        `json:"state"`
	Name        string        `json:"name"`
	Query       string        `json:"query"`
	Duration    float64       `json:"duration"`
	Labels      labels.Labels `json:"labels"`
	Annotations labels.Labels `json:"annotations"`
	Alerts      []*Alert      `json:"alerts"`
	Health      string        `json:"health"`
	LastError   string        `json:"lastError,omitempty"`
	// Type of an alertingRule is always "alerting".
	Type string `json:"type"`
}

type recordingRule struct {
	Name      string        `json:"name"`
	Query     string        `json:"query"`
	Labels    labels.Labels `json:"labels,omitempty"`
	Health    string        `json:"health"`
	LastError string        `json:"lastError,omitempty"`
	// Type of a recordingRule is always "recording".
	Type string `json:"type"`
}

func respondError(logger log.Logger, w http.ResponseWriter, msg string, errType errorType) {
	b, err := json.Marshal(&response{
		Status:    "error",
		ErrorType: errType,
		Error:     msg,
		Data:      nil,
	})

	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusInternalServerError)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
	return
}

func (r *Ruler) rules(w http.ResponseWriter, req *http.Request) {
	logger := util.WithContext(req.Context(), util.Logger)
	userID, ctx, err := user.ExtractOrgIDFromHTTPRequest(req)
	if err != nil {
		level.Error(logger).Log("msg", "error extracting org id from context", "err", err)
		respondError(logger, w, "no valid org id found", errorInternal)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rgs, err := r.GetRules(ctx, userID)

	if err != nil {
		respondError(logger, w, err.Error(), errorInternal)
		return
	}

	groups := make([]*RuleGroup, len(rgs))

	for _, g := range rgs {
		grp := RuleGroup{
			Name:     g.Name,
			File:     g.Namespace,
			Interval: g.Interval.Seconds(),
			Rules:    make([]rule, len(g.Rules)),
		}

		for i, rl := range g.Rules {
			if g.Rules[i].Alert != "" {
				alerts := make([]*Alert, len(rl.Alerts))
				for _, a := range rl.Alerts {
					alerts = append(alerts, &Alert{
						Labels:      client.FromLabelAdaptersToLabels(a.Labels),
						Annotations: client.FromLabelAdaptersToLabels(a.Annotations),
						State:       a.GetState(),
						ActiveAt:    a.ActiveAt,
						Value:       fmt.Sprintf("%f", a.Value),
					})
				}
				grp.Rules[i] = alertingRule{
					State:       rl.GetState(),
					Name:        rl.GetAlert(),
					Query:       rl.GetExpr(),
					Duration:    rl.For.Seconds(),
					Labels:      client.FromLabelAdaptersToLabels(rl.Labels),
					Annotations: client.FromLabelAdaptersToLabels(rl.Annotations),
					Alerts:      alerts,
					Health:      rl.GetHealth(),
					LastError:   rl.GetLastError(),
					Type:        "alerting",
				}
			} else {
				grp.Rules[i] = recordingRule{
					Name:      rl.GetRecord(),
					Query:     rl.GetExpr(),
					Labels:    client.FromLabelAdaptersToLabels(rl.Labels),
					Health:    rl.GetHealth(),
					LastError: rl.GetLastError(),
					Type:      "recording",
				}
			}
		}
		groups = append(groups, &grp)
	}

	b, err := json.Marshal(&response{
		Status: "success",
		Data:   &RuleDiscovery{RuleGroups: groups},
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		respondError(logger, w, "unable to marshal the requested data", errorBadData)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func (r *Ruler) alerts(w http.ResponseWriter, req *http.Request) {
	logger := util.WithContext(req.Context(), util.Logger)
	userID, ctx, err := user.ExtractOrgIDFromHTTPRequest(req)
	if err != nil {
		level.Error(logger).Log("msg", "error extracting org id from context", "err", err)
		respondError(logger, w, "no valid org id found", errorInternal)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rgs, err := r.GetRules(ctx, userID)

	if err != nil {
		respondError(logger, w, err.Error(), "internal")
		return
	}

	alerts := make([]*Alert, len(rgs))

	for _, g := range rgs {
		for _, rl := range g.Rules {
			if rl.Alert != "" {
				for _, a := range rl.Alerts {
					alerts = append(alerts, &Alert{
						Labels:      client.FromLabelAdaptersToLabels(a.Labels),
						Annotations: client.FromLabelAdaptersToLabels(a.Annotations),
						State:       a.GetState(),
						ActiveAt:    a.ActiveAt,
						Value:       fmt.Sprintf("%f", a.Value),
					})
				}
			}
		}
	}

	b, err := json.Marshal(&response{
		Status: "success",
		Data:   &AlertDiscovery{Alerts: alerts},
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		respondError(logger, w, "unable to marshal the requested data", errorBadData)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}