package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/patrk/kubectl-flink/internal/fleet"
	"github.com/patrk/kubectl-flink/internal/flink"
	"github.com/patrk/kubectl-flink/internal/kube"
	"github.com/patrk/kubectl-flink/internal/model"
	"github.com/patrk/kubectl-flink/internal/profile"
	"github.com/patrk/kubectl-flink/internal/render"
)

// target is a resolved job with the client of its context.
type target struct {
	job     *model.Job
	client  *kube.Client
	profile *profile.Profile
}

// cluster is the Flink cluster serving the job's REST API and holding its pods.
func (t *target) cluster() string {
	if t.job.Kind == model.KindSessionJob {
		return t.job.DeploymentName
	}
	return t.job.Name
}

func (t *target) String() string { return render.NewJobRef(t.job).String() }

// resolveJob finds the job an argument names in the current scopes. An exact namespace/name is read directly.
// Anything else is first looked up by the API server, as an exact name or a value of the profile's ID labels on
// the resource; only when that finds nothing are all jobs listed and matched by name, ID label, on the pod
// template too, or substring (fleet.Find). So an ID that one job carries on the resource and another only on its
// pod template names the first. A scope that cannot be read is reported when nothing is found, so a failure never
// reads as "no match".
func (o *Options) resolveJob(ctx context.Context, arg string, allNamespaces bool) (*target, error) {
	srcs, prof, err := o.sources(allNamespaces)
	if err != nil {
		return nil, err
	}
	var scopeErrs []error
	if ns, name, ok := strings.Cut(arg, "/"); ok {
		var found []*target
		for _, src := range srcs {
			if src.Err != nil {
				scopeErrs = append(scopeErrs, src.Err)
				continue
			}
			t, err := get(ctx, src, ns, name)
			if err != nil {
				scopeErrs = append(scopeErrs, fmt.Errorf("context %s: %w", src.Scope.Context, err))
			} else if t != nil {
				found = append(found, t)
			}
		}
		if len(found) == 1 {
			found[0].profile = prof
			return found[0], nil
		}
		if len(found) > 1 {
			return nil, ambiguous(arg, found)
		}
	}

	// A name or an ID the API server can find first; the whole fleet only when it finds nothing, for a part of a
	// name or an ID in a pod template label. An exact name still wins over an ID, as fleet.Find decides.
	if !strings.Contains(arg, "/") {
		m := &kube.Match{Name: arg, Labels: prof.IDLabels, Value: arg}
		t, _, err := findJob(fleet.Load(ctx, srcs, kube.Query{Match: m}), srcs, prof, arg)
		if !errors.Is(err, fleet.ErrNoMatch) {
			return t, ambiguousExit(err)
		}
	}
	t, errs, err := findJob(fleet.Load(ctx, srcs, kube.Query{}), srcs, prof, arg)
	scopeErrs = append(scopeErrs, errs...)
	switch {
	case errors.Is(err, fleet.ErrNoMatch) && len(scopeErrs) > 0:
		return nil, fmt.Errorf("%w; and not every scope could be read: %w", err, errors.Join(dedupe(scopeErrs)...))
	case errors.Is(err, fleet.ErrNoMatch) && !allNamespaces:
		return nil, fmt.Errorf("%w in %s; -A looks in every namespace", err, searched(srcs))
	}
	return t, ambiguousExit(err)
}

// findJob matches arg against the jobs of the scopes, and returns the errors of the scopes it could not read.
func findJob(scopes []fleet.Scope, srcs []fleet.Source, prof *profile.Profile, arg string) (*target, []error, error) {
	var jobs []*model.Job
	var scopeErrs []error
	clients := map[*model.Job]*kube.Client{}
	for i := range scopes {
		if scopes[i].Err != nil {
			scopeErrs = append(scopeErrs, scopes[i].Err)
			continue
		}
		for j := range scopes[i].Jobs {
			job := &scopes[i].Jobs[j]
			jobs = append(jobs, job)
			clients[job] = srcs[i].Client
		}
	}
	job, err := fleet.Find(jobs, prof.IDLabels, arg)
	if err != nil {
		return nil, scopeErrs, err
	}
	return &target{job: job, client: clients[job], profile: prof}, scopeErrs, nil
}

// ambiguousExit gives an argument that matched several jobs its own exit code.
func ambiguousExit(err error) error {
	if amb := (*fleet.AmbiguousError)(nil); errors.As(err, &amb) {
		return &exitError{code: ExitAmbiguous, err: err}
	}
	return err
}

// searched names the namespaces a job was looked for in, for a search that found nothing.
func searched(srcs []fleet.Source) string {
	var ns []string
	for _, s := range srcs {
		if s.Scope.AllNamespaces {
			return "every namespace"
		}
		for _, n := range s.Scope.Namespaces {
			ns = append(ns, cmp.Or(n, s.ContextNamespace, "default"))
		}
		if len(s.Scope.Namespaces) == 0 {
			ns = append(ns, cmp.Or(s.ContextNamespace, "default"))
		}
	}
	slices.Sort(ns)
	ns = slices.Compact(ns)
	if len(ns) == 1 {
		return "namespace " + ns[0]
	}
	return "namespaces " + strings.Join(ns, ", ")
}

func ambiguous(arg string, found []*target) error {
	jobs := make([]*model.Job, len(found))
	for i, t := range found {
		jobs[i] = t.job
	}
	return &exitError{code: ExitAmbiguous, err: &fleet.AmbiguousError{Arg: arg, Candidates: jobs}}
}

// dedupe drops repeated errors: the same scope can fail both its direct read and its list.
func dedupe(errs []error) []error {
	seen := map[string]bool{}
	var out []error
	for _, e := range errs {
		if !seen[e.Error()] {
			seen[e.Error()] = true
			out = append(out, e)
		}
	}
	return out
}

// get reads namespace/name as a FlinkDeployment, then as a FlinkSessionJob; nil when it is neither.
func get(ctx context.Context, src fleet.Source, ns, name string) (*target, error) {
	obj, err := src.Client.Get(ctx, kube.Deployments, ns, name)
	if err == nil {
		job := model.FromDeployment(src.Scope.Context, obj.(*kube.FlinkDeployment))
		job.Owner = model.InferOwner(&job, src.Owners)
		return &target{job: &job, client: src.Client}, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	obj, err = src.Client.Get(ctx, kube.SessionJobs, ns, name)
	if err == nil {
		job := model.FromSessionJob(src.Scope.Context, obj.(*kube.FlinkSessionJob))
		job.Owner = model.InferOwner(&job, src.Owners)
		return &target{job: &job, client: src.Client}, nil
	}
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return nil, err
}

// access returns how to reach the job's JobManager: --rest-url, or a port-forward to a ready JobManager pod.
func (t *target) access(restURL string) (access flink.Access, closeFn func()) {
	if restURL != "" {
		return flink.Direct(restURL), func() {}
	}
	pool := newPool(t.client, 1)
	return pool, pool.Close
}

// newPool keeps up to size port-forwards to the ready JobManager pods the client can reach.
func newPool(c *kube.Client, size int) *flink.Pool {
	return flink.NewPool(flink.NewForwarder(c.Config()), func(ctx context.Context, ns, cluster string) ([]string, error) {
		pods, err := c.ComponentPods(ctx, ns, cluster, kube.ComponentJM)
		if err != nil {
			return nil, err
		}
		var ready []string
		for i := range pods {
			if pods[i].Ready() {
				ready = append(ready, pods[i].Name)
			}
		}
		return ready, nil
	}, size)
}

// ErrJobManagerDown means the job's JobManager is not running, so there is no REST API to ask.
var ErrJobManagerDown = errors.New("the JobManager is not running")

// checkJobManager fails early, with the operator's own error, when the resource says the JobManager is down.
func (t *target) checkJobManager() error {
	j := t.job
	if j.Kind != model.KindDeployment {
		return nil
	}
	switch j.JMStatus {
	case "MISSING", "ERROR", "DEPLOYING":
	default:
		return nil
	}
	msg := fmt.Sprintf("%v: %s is %s", ErrJobManagerDown, t, j.JMStatus)
	if j.Error != nil {
		msg += ": " + j.Error.Summary(200)
	}
	return fmt.Errorf("%s\nsee its log with: kubectl flink logs %s/%s --previous", msg, j.Namespace, j.Name)
}

// restFlags are the flags of every command that reads the JobManager REST API.
type restFlags struct {
	allNamespaces bool
	restURL       string
	output        string
	limit         int
}

func (f *restFlags) register(cmd *cobra.Command, limitUsage string) {
	cmd.Flags().BoolVarP(&f.allNamespaces, "all-namespaces", "A", false, "Look for the job in all namespaces")
	cmd.Flags().StringVar(&f.restURL, "rest-url", "", "JobManager REST URL to use instead of a port-forward, such as http://localhost:8081")
	cmd.Flags().StringVarP(&f.output, "output", "o", "", "Output format: json, yaml")
	cmd.Flags().IntVar(&f.limit, "limit", 10, limitUsage)
}

func (f *restFlags) validate() error {
	if f.limit < 1 {
		return &exitError{code: ExitUsage, err: fmt.Errorf("--limit must be at least 1, got %d", f.limit)}
	}
	switch f.output {
	case "", "json", "yaml":
		return nil
	}
	return &exitError{code: ExitUsage, err: fmt.Errorf("unknown output %q, use json or yaml", f.output)}
}

// withJobManager resolves the job and runs fn against its JobManager with the command's context, refusing early
// when the resource says the JobManager is down or the job has no ID yet.
func (o *Options) withJobManager(ctx context.Context, arg string, f *restFlags, fn func(context.Context, *target, *flink.Client) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t, err := o.resolveJob(ctx, arg, f.allNamespaces)
	if err != nil {
		return err
	}
	return t.withJobManager(ctx, f.restURL, func(c *flink.Client) error { return fn(ctx, t, c) })
}

// withJobManager runs fn against the job's JobManager, when it is up or restURL is given.
func (t *target) withJobManager(ctx context.Context, restURL string, fn func(*flink.Client) error) error {
	if restURL == "" {
		if err := t.checkJobManager(); err != nil {
			return err
		}
	}
	if t.job.JobID == "" {
		return fmt.Errorf("%s has no Flink job ID in its status yet", t)
	}
	access, closeFn := t.access(restURL)
	defer closeFn()
	return access.Do(ctx, t.job.Namespace, t.cluster(), fn)
}
