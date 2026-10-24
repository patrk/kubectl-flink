// Package render turns fleet rows into tables, JSON, YAML and names.
package render

import (
	"cmp"
	"fmt"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/model"
)

// APIVersion versions the JSON and YAML output.
const APIVersion = "kubectl-flink/v1alpha1"

// List is the -o json and -o yaml document of ls.
type List struct {
	APIVersion string        `json:"apiVersion"`
	Kind       string        `json:"kind"`
	Items      []Item        `json:"items"`
	Scopes     []ScopeStatus `json:"scopes"`
	Summary    Summary       `json:"summary"`
}

// Item is one job in the JSON and YAML output: a stable contract, not the internal model.
type Item struct {
	Context        string          `json:"context,omitempty"`
	Namespace      string          `json:"namespace"`
	Name           string          `json:"name"`
	Kind           string          `json:"kind"`
	Health         model.Health    `json:"health"`
	Findings       []model.Finding `json:"findings,omitempty"`
	Lifecycle      string          `json:"lifecycle,omitempty"`
	JobState       string          `json:"jobState,omitempty"`
	JobManager     string          `json:"jobManager,omitempty"`
	StateSince     *time.Time      `json:"stateSince,omitempty"`
	JobID          string          `json:"jobId,omitempty"`
	SessionCluster string          `json:"sessionCluster,omitempty"`
	Error          string          `json:"error,omitempty"`
	TaskManagers   *Replicas       `json:"taskManagers,omitempty"`
	JMRestarts     *int32          `json:"jobManagerRestarts,omitempty"`
	Drift          bool            `json:"drift,omitempty"`
	FlinkVersion   string          `json:"flinkVersion,omitempty"`
	Image          string          `json:"image,omitempty"`
	Owners         []string        `json:"owners,omitempty"`
}

// Replicas is ready against desired.
type Replicas struct {
	Ready   int   `json:"ready"`
	Desired int32 `json:"desired"`
}

// ScopeStatus reports how a scope was read.
type ScopeStatus struct {
	Context    string   `json:"context"`
	Namespaces []string `json:"namespaces,omitempty"`
	All        bool     `json:"allNamespaces,omitempty"`
	Note       string   `json:"note,omitempty"`
	Error      string   `json:"error,omitempty"`
	WatchError string   `json:"watchError,omitempty"`
	PodsUnread []string `json:"podsUnread,omitempty"`
}

// NewScopeStatus converts a scope.
func NewScopeStatus(s *fleet.Scope) ScopeStatus {
	st := ScopeStatus{Context: s.Context, Namespaces: s.Namespaces, All: s.AllNamespaces, Note: s.Note, PodsUnread: s.PodsUnread}
	if s.Err != nil {
		st.Error = s.Err.Error()
	}
	if s.WatchErr != nil {
		st.WatchError = s.WatchErr.Error()
	}
	return st
}

// Summary counts the jobs by health and adds up their pods' requests.
type Summary struct {
	Jobs   int            `json:"jobs"`
	Health map[string]int `json:"health"`
	Pods   int            `json:"pods"`
	CPU    string         `json:"cpuRequests"`
	Memory string         `json:"memoryRequests"`
}

// NewItem converts a row.
func NewItem(r *fleet.Row) Item {
	j := r.Job
	it := Item{
		Context: j.Context, Namespace: j.Namespace, Name: j.Name, Kind: string(j.Kind),
		Health: r.Health, Findings: r.Findings,
		Lifecycle: j.Lifecycle, JobState: j.JobState, JobManager: j.JMStatus, JobID: j.JobID,
		SessionCluster: j.DeploymentName, Drift: j.Drift(), FlinkVersion: flinkVersion(j), Image: j.Image,
		Owners: j.SpecOwners,
	}
	if t := j.StateSince(); !t.IsZero() {
		t = t.UTC()
		it.StateSince = &t
	}
	if j.Error != nil {
		it.Error = j.Error.Summary(200)
	}
	if j.Pods != nil {
		restarts := j.Pods.JM.Restarts
		it.JMRestarts = &restarts
		if j.Kind == model.KindDeployment && !j.SessionCluster {
			it.TaskManagers = &Replicas{Ready: j.Pods.TM.Ready, Desired: j.TMReplicas}
		}
	}
	return it
}

// NewList builds the JSON and YAML document.
func NewList(rows []fleet.Row, scopes []fleet.Scope) List {
	l := List{APIVersion: APIVersion, Kind: "JobList", Items: make([]Item, len(rows)), Summary: Summarise(rows)}
	for i := range rows {
		l.Items[i] = NewItem(&rows[i])
	}
	for i := range scopes {
		l.Scopes = append(l.Scopes, NewScopeStatus(&scopes[i]))
	}
	return l
}

// Summarise counts rows by health and sums the requests of their pods, counting each cluster once.
func Summarise(rows []fleet.Row) Summary {
	s := Summary{Jobs: len(rows), Health: map[string]int{}}
	var cpu, mem resource.Quantity
	seen := map[*model.ClusterPods]bool{}
	for i := range rows {
		s.Health[rows[i].Health.String()]++
		cp := rows[i].Job.Pods
		if cp == nil || seen[cp] {
			continue
		}
		seen[cp] = true
		s.Pods += cp.JM.Total + cp.TM.Total
		cpu.Add(cp.CPU)
		mem.Add(cp.Memory)
	}
	s.CPU, s.Memory = cpu.String(), mem.String()
	return s
}

// Line is the one-line summary printed after a table.
func (s *Summary) Line() string {
	var parts []string
	for h := model.Failed; h >= model.OK; h-- {
		if n := s.Health[h.String()]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, h))
		}
	}
	jobs := "jobs"
	if s.Jobs == 1 {
		jobs = "job"
	}
	line := fmt.Sprintf("%d %s", s.Jobs, jobs)
	if len(parts) > 0 {
		line += ": " + strings.Join(parts, ", ")
	}
	return line + fmt.Sprintf(" · %d pods · requests %s CPU, %s memory", s.Pods, s.CPU, s.Memory)
}

// Notes are the lines that explain how scopes were read: fallbacks and unread pods.
func Notes(scopes []fleet.Scope) []string {
	var out []string
	for i := range scopes {
		s := &scopes[i]
		if s.Note != "" {
			out = append(out, s.Note)
		}
		if len(s.PodsUnread) > 0 {
			ns := make([]string, len(s.PodsUnread))
			for i, n := range s.PodsUnread {
				ns[i] = cmp.Or(n, "all namespaces")
			}
			sort.Strings(ns)
			out = append(out, fmt.Sprintf("cannot list pods in context %s, %s; TaskManager checks skipped there",
				s.Context, strings.Join(ns, ", ")))
		}
	}
	return out
}

// flinkVersion is the version the cluster reports, else the one the spec asks for.
func flinkVersion(j *model.Job) string { return cmp.Or(j.FlinkRuntime, j.FlinkVersion) }

// JobRef names a job in the output of single-job commands.
type JobRef struct {
	Context   string `json:"context,omitempty"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	JobID     string `json:"jobId,omitempty"`
}

// NewJobRef names j.
func NewJobRef(j *model.Job) JobRef {
	return JobRef{Context: j.Context, Namespace: j.Namespace, Name: j.Name, Kind: string(j.Kind), JobID: j.JobID}
}

func (r JobRef) String() string {
	s := fmt.Sprintf("%s %s/%s", r.Kind, r.Namespace, r.Name)
	if r.Context != "" {
		s += " in context " + r.Context
	}
	if r.JobID != "" {
		s += ", job " + r.JobID
	}
	return s
}
