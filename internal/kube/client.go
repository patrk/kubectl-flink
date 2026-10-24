package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/patrk/kubectl-flink/internal/version"
)

// PageSize is the LIST page size. Flink CRs are large, so pages stay well below the API server's default 500.
const PageSize = 250

// Resource is one kind the tool lists and watches.
type Resource struct {
	GVR     schema.GroupVersionResource
	newObj  func() runtime.Object
	newList func() runtime.Object
	core    bool
}

// The resources the tool reads.
var (
	Deployments = Resource{
		GVR:     FlinkGroupVersion.WithResource("flinkdeployments"),
		newObj:  func() runtime.Object { return &FlinkDeployment{} },
		newList: func() runtime.Object { return &FlinkDeploymentList{} },
	}
	SessionJobs = Resource{
		GVR:     FlinkGroupVersion.WithResource("flinksessionjobs"),
		newObj:  func() runtime.Object { return &FlinkSessionJob{} },
		newList: func() runtime.Object { return &FlinkSessionJobList{} },
	}
	Pods = Resource{
		GVR:     CoreGroupVersion.WithResource("pods"),
		newObj:  func() runtime.Object { return &Pod{} },
		newList: func() runtime.Object { return &PodList{} },
		core:    true,
	}
)

// Client lists and watches the minimal types in one context.
type Client struct {
	cfg         *rest.Config
	flink, core rest.Interface
}

// NewClient builds REST clients for the Flink group and core/v1 that decode into the minimal types.
func NewClient(cfg *rest.Config) (*Client, error) {
	flink, err := restClient(cfg, FlinkGroupVersion, "/apis")
	if err != nil {
		return nil, err
	}
	core, err := restClient(cfg, CoreGroupVersion, "/api")
	if err != nil {
		return nil, err
	}
	return &Client{cfg: rest.CopyConfig(cfg), flink: flink, core: core}, nil
}

// Config is the context's REST config, for port-forwarding.
func (c *Client) Config() *rest.Config { return rest.CopyConfig(c.cfg) }

func restClient(cfg *rest.Config, gv schema.GroupVersion, apiPath string) (*rest.RESTClient, error) {
	c := rest.CopyConfig(cfg)
	c.GroupVersion = &gv
	c.APIPath = apiPath
	c.NegotiatedSerializer = Codecs
	// Custom resources are served as JSON only; asking for anything else costs a negotiation round.
	c.ContentType = runtime.ContentTypeJSON
	c.AcceptContentTypes = runtime.ContentTypeJSON
	if c.UserAgent == "" {
		c.UserAgent = "kubectl-flink/" + version.Version
	}
	// Namespaces are listed in parallel; client-go's default of 5 QPS would serialise them.
	if c.QPS == 0 {
		c.QPS, c.Burst = 50, 100
	}
	return rest.RESTClientFor(c)
}

func (c *Client) rest(r Resource) rest.Interface {
	if r.core {
		return c.core
	}
	return c.flink
}

func (c *Client) get(r Resource, namespace string, opts *metav1.ListOptions) *rest.Request {
	return c.rest(r).Get().
		NamespaceIfScoped(namespace, namespace != "").
		Resource(r.GVR.Resource).
		VersionedParams(opts, metav1.ParameterCodec)
}

// listPage decodes the body with encoding/json instead of the codec: the type is known, and the codec scans
// the whole body once more to find the kind, which doubles the decode time of a page (BenchmarkDecodeList).
// API errors are still decoded into Status by the REST client.
func (c *Client) listPage(ctx context.Context, r Resource, namespace string, opts metav1.ListOptions) (runtime.Object, error) {
	raw, err := rawBody(c.get(r, namespace, &opts).Do(ctx))
	if err != nil {
		return nil, err
	}
	obj := r.newList()
	if err := json.Unmarshal(raw, obj); err != nil {
		return nil, fmt.Errorf("decode %s: %w", r.GVR.Resource, err)
	}
	return obj, nil
}

func (c *Client) watch(ctx context.Context, r Resource, namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.get(r, namespace, &opts).Watch(ctx)
}

// pagedList is a list type that pages can be appended to.
type pagedList interface {
	runtime.Object
	GetContinue() string
	SetContinue(string)
	SetRemainingItemCount(*int64)
	appendPage(page runtime.Object)
}

func (l *FlinkDeploymentList) appendPage(p runtime.Object) {
	l.Items = append(l.Items, p.(*FlinkDeploymentList).Items...)
}

func (l *FlinkSessionJobList) appendPage(p runtime.Object) {
	l.Items = append(l.Items, p.(*FlinkSessionJobList).Items...)
}

func (l *PodList) appendPage(p runtime.Object) { l.Items = append(l.Items, p.(*PodList).Items...) }

// List lists r in namespace ("" for all namespaces) in pages of PageSize and returns one typed list. If the
// continue token expires between pages, the list starts over once.
func (c *Client) List(ctx context.Context, r Resource, namespace, labelSelector string) (runtime.Object, error) {
	return c.list(ctx, r, namespace, metav1.ListOptions{LabelSelector: labelSelector})
}

func (c *Client) list(ctx context.Context, r Resource, namespace string, opts metav1.ListOptions) (runtime.Object, error) {
	obj, err := c.listAll(ctx, r, namespace, opts)
	if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
		obj, err = c.listAll(ctx, r, namespace, opts)
	}
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", r.GVR.Resource, err)
	}
	return obj, nil
}

func (c *Client) listAll(ctx context.Context, r Resource, namespace string, opts metav1.ListOptions) (runtime.Object, error) {
	opts.Limit = PageSize
	var all pagedList
	for {
		page, err := c.listPage(ctx, r, namespace, opts)
		if err != nil {
			return nil, err
		}
		pl := page.(pagedList)
		if all == nil {
			all = pl
		} else {
			all.appendPage(pl)
		}
		if opts.Continue = pl.GetContinue(); opts.Continue == "" {
			// The merged list is complete; page 1's continue token must not suggest otherwise.
			all.SetContinue("")
			all.SetRemainingItemCount(nil)
			return all, nil
		}
	}
}

// rawBody returns the response body undecoded. On failure it returns Result.Error, which decodes the API
// server's Status and keeps its message ("User x cannot list ..."); Raw alone would give a generic "unknown".
func rawBody(res rest.Result) ([]byte, error) {
	raw, err := res.Raw()
	if err != nil {
		return nil, res.Error()
	}
	return raw, nil
}

// GetRaw reads one object of r as the API server sends it, every field included.
func (c *Client) GetRaw(ctx context.Context, r Resource, namespace, name string) ([]byte, error) {
	return rawBody(c.rest(r).Get().Namespace(namespace).Resource(r.GVR.Resource).Name(name).Do(ctx))
}

// ListRaw lists r as the API server sends it, in one request; for the few objects of one job.
func (c *Client) ListRaw(ctx context.Context, r Resource, namespace, labelSelector, fieldSelector string) ([]byte, error) {
	opts := metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: fieldSelector}
	return rawBody(c.get(r, namespace, &opts).Do(ctx))
}

// Get reads one object of r. A missing object is an apierrors NotFound.
func (c *Client) Get(ctx context.Context, r Resource, namespace, name string) (runtime.Object, error) {
	raw, err := rawBody(c.rest(r).Get().Namespace(namespace).Resource(r.GVR.Resource).Name(name).Do(ctx))
	if err != nil {
		return nil, err
	}
	obj := r.newObj()
	if err := json.Unmarshal(raw, obj); err != nil {
		return nil, fmt.Errorf("decode %s %s/%s: %w", r.GVR.Resource, namespace, name, err)
	}
	return obj, nil
}

// ComponentPods returns the pods of one component (ComponentJM or ComponentTM) of a Flink cluster.
func (c *Client) ComponentPods(ctx context.Context, namespace, cluster, component string) ([]Pod, error) {
	sel := FlinkPodSelector + "," + LabelApp + "=" + cluster + "," + LabelComponent + "=" + component
	obj, err := c.List(ctx, Pods, namespace, sel)
	if err != nil {
		return nil, err
	}
	return obj.(*PodList).Items, nil
}

// LogOptions select a container log.
type LogOptions struct {
	Container    string
	Follow       bool
	Previous     bool
	TailLines    int64
	SinceSeconds int64
}

// Logs streams a container's log through the pods/log subresource.
func (c *Client) Logs(ctx context.Context, namespace, pod string, o LogOptions) (io.ReadCloser, error) {
	req := c.core.Get().Namespace(namespace).Resource("pods").Name(pod).SubResource("log")
	if o.Container != "" {
		req = req.Param("container", o.Container)
	}
	if o.Follow {
		req = req.Param("follow", "true")
	}
	if o.Previous {
		req = req.Param("previous", "true")
	}
	if o.TailLines > 0 {
		req = req.Param("tailLines", strconv.FormatInt(o.TailLines, 10))
	}
	if o.SinceSeconds > 0 {
		req = req.Param("sinceSeconds", strconv.FormatInt(o.SinceSeconds, 10))
	}
	return req.Stream(ctx)
}

// Probe lists at most one item, to learn whether r can be listed in namespace without transferring it.
func (c *Client) Probe(ctx context.Context, r Resource, namespace string) error {
	_, err := c.listPage(ctx, r, namespace, metav1.ListOptions{Limit: 1})
	return err
}

// ListWatch lists and watches r in namespace for an informer. The reflector streams the initial state with
// WatchList where the server supports it and falls back to a paginated LIST otherwise.
func (c *Client) ListWatch(r Resource, namespace, labelSelector string) *cache.ListWatch {
	return &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			opts.LabelSelector = labelSelector
			return c.listPage(ctx, r, namespace, opts)
		},
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			opts.LabelSelector = labelSelector
			return c.watch(ctx, r, namespace, opts)
		},
	}
}

// Informer builds an informer over r in namespace ("" for all namespaces). It does not resync: every change
// arrives as a watch event, and a periodic resync would only re-derive unchanged jobs.
func (c *Client) Informer(r Resource, namespace, labelSelector string, indexers cache.Indexers) cache.SharedIndexInformer {
	if indexers == nil {
		indexers = cache.Indexers{}
	}
	return cache.NewSharedIndexInformer(c.ListWatch(r, namespace, labelSelector), r.newObj(), 0, indexers)
}
