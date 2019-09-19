package ruler

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/prometheus/pkg/rulefmt"

	store "github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"
)

var (
	// ErrNoNamespace signals the requested namespace does not exist
	ErrNoNamespace = errors.New("a namespace must be provided in the url")
	// ErrNoGroupName signals a group name url parameter was not found
	ErrNoGroupName = errors.New("a matching group name must be provided in the url")
	// ErrNoRuleGroups signals the rule group requested does not exist
	ErrNoRuleGroups = errors.New("no rule groups found")
	// ErrNoUserID is returned when no user ID is provided
	ErrNoUserID = errors.New("no id provided")
)

// RuleNamespace is used to parse a slightly modified prometheus
// rule file format, if no namespace is set, the default namespace
// is used
type RuleNamespace struct {
	// Namespace field only exists for setting namespace in namespace body instead of file name
	Namespace string `yaml:"namespace,omitempty"`

	Groups []rulefmt.RuleGroup `yaml:"groups"`
}

// Validate each rule in the rule namespace is valid
func (r RuleNamespace) Validate() []error {
	set := map[string]struct{}{}
	var errs []error

	for _, g := range r.Groups {
		if g.Name == "" {
			errs = append(errs, fmt.Errorf("Groupname should not be empty"))
		}

		if _, ok := set[g.Name]; ok {
			errs = append(
				errs,
				fmt.Errorf("groupname: \"%s\" is repeated in the same namespace", g.Name),
			)
		}

		set[g.Name] = struct{}{}

		errs = append(errs, ValidateRuleGroup(g)...)
	}

	return errs
}

// ValidateRuleGroup validates a rulegroup
func ValidateRuleGroup(g rulefmt.RuleGroup) []error {
	var errs []error
	for i, r := range g.Rules {
		for _, err := range r.Validate() {
			var ruleName string
			if r.Alert != "" {
				ruleName = r.Alert
			} else {
				ruleName = r.Record
			}
			errs = append(errs, &rulefmt.Error{
				Group:    g.Name,
				Rule:     i,
				RuleName: ruleName,
				Err:      err,
			})
		}
	}

	return errs
}

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
		{"list_rules", "GET", "/api/prom/rules", r.listRules},
		{"getRuleNamespace", "GET", "/api/prom/rules/{namespace}", r.listRules},
		{"get_rulegroup", "GET", "/api/prom/rules/{namespace}/{groupName}", r.getRuleGroup},
		{"set_rulegroup", "POST", "/api/prom/rules/{namespace}", r.createRuleGroup},
		{"delete_rulegroup", "DELETE", "/api/prom/rules/{namespace}/{groupName}", r.deleteRuleGroup},
	} {
		level.Debug(util.Logger).Log("msg", "ruler: registering route", "name", route.name, "method", route.method, "path", route.path)
		router.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
}

func (r *Ruler) listRules(w http.ResponseWriter, req *http.Request) {
	logger := util.WithContext(req.Context(), util.Logger)
	userID, _, err := user.ExtractOrgIDFromHTTPRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	if userID == "" {
		http.Error(w, ErrNoUserID.Error(), http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(req)

	namespace := vars["namespace"]
	if namespace != "" {
		level.Debug(logger).Log("msg", "retrieving rule groups with namespace", "userID", userID, "namespace", namespace)
	}

	level.Debug(logger).Log("msg", "retrieving rule groups from rule store", "userID", userID)
	rgs, err := r.store.ListRuleGroups(req.Context(), userID, namespace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(logger).Log("msg", "retrieved rule groups from rule store", "userID", userID, "num_namespaces", len(rgs))

	if len(rgs) == 0 {
		level.Info(logger).Log("msg", "no rule groups found", "userID", userID)
		http.Error(w, ErrNoRuleGroups.Error(), http.StatusNotFound)
		return
	}

	formatted := rgs.Formatted()

	d, err := yaml.Marshal(&formatted)
	if err != nil {
		level.Error(logger).Log("msg", "error marshalling yaml rule groups", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if _, err := w.Write(d); err != nil {
		level.Error(logger).Log("msg", "error writing yaml response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (r *Ruler) getRuleGroup(w http.ResponseWriter, req *http.Request) {
	logger := util.WithContext(req.Context(), util.Logger)

	userID, _, err := user.ExtractOrgIDFromHTTPRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	if userID == "" {
		http.Error(w, ErrNoUserID.Error(), http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(req)
	namespace, exists := vars["namespace"]
	if !exists {
		http.Error(w, ErrNoNamespace.Error(), http.StatusUnauthorized)
		return
	}

	groupName, exists := vars["groupName"]
	if !exists {
		http.Error(w, ErrNoGroupName.Error(), http.StatusUnauthorized)
		return
	}

	rg, err := r.store.GetRuleGroup(req.Context(), userID, namespace, groupName)
	if err != nil {
		if err == store.ErrGroupNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	formattedRG := store.FromProto(rg)

	d, err := yaml.Marshal(&formattedRG)
	if err != nil {
		level.Error(logger).Log("msg", "error marshalling yaml rule groups", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if _, err := w.Write(d); err != nil {
		level.Error(logger).Log("msg", "error writing yaml response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (r *Ruler) createRuleGroup(w http.ResponseWriter, req *http.Request) {
	logger := util.WithContext(req.Context(), util.Logger)
	userID, _, err := user.ExtractOrgIDFromHTTPRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	if userID == "" {
		http.Error(w, ErrNoUserID.Error(), http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(req)

	namespace := vars["namespace"]
	if namespace == "" {
		level.Error(logger).Log("err", "no namespace provided with rule group")
		http.Error(w, ErrNoNamespace.Error(), http.StatusBadRequest)
		return
	}

	payload, err := ioutil.ReadAll(req.Body)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(logger).Log("msg", "attempting to unmarshal rulegroup", "userID", userID, "group", string(payload))

	rg := rulefmt.RuleGroup{}
	err = yaml.Unmarshal(payload, &rg)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	errs := ValidateRuleGroup(rg)
	if len(errs) > 0 {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, errs[0].Error(), http.StatusBadRequest)
		return
	}

	rgProto := store.ToProto(userID, namespace, rg)

	level.Debug(logger).Log("msg", "attempting to store rulegroup", "userID", userID, "group", rgProto.String())
	err = r.store.SetRuleGroup(req.Context(), userID, namespace, rgProto)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return a status accepted because the rule has been stored and queued for polling, but is not currently active
	w.WriteHeader(http.StatusAccepted)
}

func (r *Ruler) deleteRuleGroup(w http.ResponseWriter, req *http.Request) {
	logger := util.WithContext(req.Context(), util.Logger)
	userID, _, err := user.ExtractOrgIDFromHTTPRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	if userID == "" {
		http.Error(w, ErrNoUserID.Error(), http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(req)
	namespace, exists := vars["namespace"]
	if !exists {
		http.Error(w, ErrNoNamespace.Error(), http.StatusUnauthorized)
		return
	}

	groupName, exists := vars["groupName"]
	if !exists {
		http.Error(w, ErrNoGroupName.Error(), http.StatusUnauthorized)
		return
	}

	err = r.store.DeleteRuleGroup(req.Context(), userID, namespace, groupName)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return a status accepted because the rule has been stored and queued for polling, but is not currently active
	w.WriteHeader(http.StatusAccepted)
}
