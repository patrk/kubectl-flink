// Package kubetest is an in-process fake of the Kubernetes API for the resources kubectl-flink reads: list with
// pagination, watch, WatchList, label selectors, Forbidden and NotFound. Tests of every layer above kube use it.
package kubetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
)

// listKinds are the kind and apiVersion a real API server puts on list responses.
var listKinds = map[string][2]string{
	"flinkdeployments":    {"FlinkDeploymentList", "flink.apache.org/v1beta1"},
	"flinksessionjobs":    {"FlinkSessionJobList", "flink.apache.org/v1beta1"},
	"pods":                {"PodList", "v1"},
	"events":              {"EventList", "v1"},
	"configmaps":          {"ConfigMapList", "v1"},
	"flinkstatesnapshots": {"FlinkStateSnapshotList", "flink.apache.org/v1beta1"},
}

type item struct {
	ns, name string
	labels   map[string]string
	fields   map[string]string // field selector values: metadata.name, and involvedObject.uid, reason and type for events
	raw      []byte
}

type watcher struct {
	resource, ns string
	ch           chan []byte
}

// API is the fake. Its exported fields may be changed with Set while requests are in flight.
type API struct {
	t  testing.TB
	mu sync.Mutex
	rv int
	// items per resource ("flinkdeployments", "flinksessionjobs", "pods"), serialised once when added.
	items    map[string][]*item
	watchers map[*watcher]struct{}
	queries  []Request

	// Forbidden holds "resource/namespace" pairs answered with 403; namespace "" is cluster scope.
	Forbidden map[string]bool
	// NoWatchList rejects sendInitialEvents the way an API server without the WatchList feature does.
	NoWatchList bool
	// NotFound answers every request with 404, as when the CRDs are not installed.
	NotFound bool
	// BrokenInformers fails watches and full lists with 500 while one-item probes still succeed, as an API
	// server does when it is overloaded or an object cannot be served.
	BrokenInformers bool
	// ForbidContinued answers every request for a next page (one with a continue token) with 403.
	ForbidContinued bool
	// Latency delays every response, to model the round trip to a real API server.
	Latency time.Duration
	// Logs holds container logs by "namespace/pod", and of the previous container by "namespace/pod:previous",
	// served by the pods/log subresource.
	Logs map[string]string
}

// Request is one request the fake received.
type Request struct {
	Path  string
	Query url.Values
}

// New returns an empty fake.
func New(t testing.TB) *API {
	return &API{t: t, items: map[string][]*item{}, watchers: map[*watcher]struct{}{}, Forbidden: map[string]bool{}}
}

// Start serves the fake until the test ends and returns a REST config pointing at it.
func (f *API) Start() *rest.Config {
	srv := httptest.NewServer(f)
	f.t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}
}

// Set changes the fake's behaviour while requests may be in flight.
func (f *API) Set(change func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change()
}

// ReadFixture returns the object in a captured fixture; a captured list (such as pods.json) yields its first item.
func ReadFixture(t testing.TB, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path) // #nosec G304 -- test fixtures
	if err != nil {
		t.Fatal(err)
	}
	var o map[string]any
	if err := json.Unmarshal(b, &o); err != nil {
		t.Fatal(err)
	}
	if items, ok := o["items"].([]any); ok {
		o = items[0].(map[string]any)
	}
	return o
}

// Add stores n copies of a fixture object in namespace ns, named prefix-0 to prefix-(n-1). edit, if not nil,
// changes each copy before it is stored.
func (f *API) Add(resource, fixture, ns, prefix string, n int, edit func(i int, obj map[string]any)) {
	f.t.Helper()
	base, err := json.Marshal(ReadFixture(f.t, fixture))
	if err != nil {
		f.t.Fatal(err)
	}
	for i := range n {
		var o map[string]any
		_ = json.Unmarshal(base, &o)
		md := o["metadata"].(map[string]any)
		md["name"] = fmt.Sprintf("%s-%d", prefix, i)
		md["namespace"] = ns
		if edit != nil {
			edit(i, o)
		}
		f.Put(resource, o)
	}
}

// PutAll stores every item of a captured list, such as events.json, keeping names and namespaces.
func (f *API) PutAll(resource, fixture string) {
	f.t.Helper()
	b, err := os.ReadFile(fixture) // #nosec G304 -- test fixtures
	if err != nil {
		f.t.Fatal(err)
	}
	var l struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(b, &l); err != nil {
		f.t.Fatal(err)
	}
	for _, o := range l.Items {
		f.Put(resource, o)
	}
}

// Put stores obj, replacing an object of the same namespace and name, and sends it to open watches as ADDED or
// MODIFIED.
func (f *API) Put(resource string, obj map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rv++
	md := obj["metadata"].(map[string]any)
	md["resourceVersion"] = strconv.Itoa(f.rv)
	it := &item{ns: md["namespace"].(string), name: md["name"].(string), labels: map[string]string{}}
	if l, ok := md["labels"].(map[string]any); ok {
		for k, v := range l {
			it.labels[k], _ = v.(string)
		}
	}
	it.fields = map[string]string{"metadata.name": it.name}
	if ref, ok := obj["involvedObject"].(map[string]any); ok {
		it.fields["involvedObject.uid"] = fmt.Sprint(ref["uid"])
		it.fields["reason"], _ = obj["reason"].(string)
		it.fields["type"], _ = obj["type"].(string)
	}
	var err error
	if it.raw, err = json.Marshal(obj); err != nil {
		f.t.Fatal(err)
	}
	typ := "ADDED"
	items := f.items[resource]
	if i := slices.IndexFunc(items, func(o *item) bool { return o.ns == it.ns && o.name == it.name }); i >= 0 {
		items[i], typ = it, "MODIFIED"
	} else {
		f.items[resource] = append(items, it)
	}
	ev := []byte(`{"type":"` + typ + `","object":` + string(it.raw) + "}\n")
	for w := range f.watchers {
		if w.resource == resource && (w.ns == "" || w.ns == it.ns) {
			w.ch <- ev
		}
	}
}

// Requests returns the requests received so far.
func (f *API) Requests() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.queries)
}

// Count counts the requests that match pred.
func (f *API) Count(pred func(Request) bool) int {
	n := 0
	for _, r := range f.Requests() {
		if pred(r) {
			n++
		}
	}
	return n
}

func (f *API) status(w http.ResponseWriter, code int, reason, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": code, "reason": reason, "message": msg,
	})
}

// matches reports whether set satisfies a label selector, parsed as the API server does.
func matches(set map[string]string, selector string) bool {
	sel, err := labels.Parse(selector)
	return err == nil && sel.Matches(labels.Set(set))
}

// matchesFields supports the key=value field selectors the tool sends.
func matchesFields(fields map[string]string, selector string) bool {
	if selector == "" {
		return true
	}
	for _, term := range strings.Split(selector, ",") {
		k, v, _ := strings.Cut(term, "=")
		if fields[k] != v {
			return false
		}
	}
	return true
}

// ServeHTTP implements http.Handler.
func (f *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	path := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/apis/flink.apache.org/v1beta1/"), "/api/v1/")
	ns := ""
	if nsPath, ok := strings.CutPrefix(path, "namespaces/"); ok {
		ns, path, _ = strings.Cut(nsPath, "/")
	}
	resource, name, _ := strings.Cut(path, "/")
	name, sub, _ := strings.Cut(name, "/")

	f.mu.Lock()
	f.queries = append(f.queries, Request{Path: r.URL.Path, Query: q})
	notFound, forbidden := f.NotFound, f.Forbidden[resource+"/"+ns] || f.ForbidContinued && q.Get("continue") != ""
	broken := f.BrokenInformers && q.Get("limit") != "1"
	var items []*item
	for _, it := range f.items[resource] {
		if (ns == "" || it.ns == ns) && matches(it.labels, q.Get("labelSelector")) && matchesFields(it.fields, q.Get("fieldSelector")) {
			items = append(items, it)
		}
	}
	rv := strconv.Itoa(f.rv)
	latency := f.Latency
	f.mu.Unlock()

	time.Sleep(latency)
	switch {
	case notFound:
		f.status(w, http.StatusNotFound, "NotFound", "the server could not find the requested resource")
	case name != "" && !forbidden:
		f.object(w, q, resource, ns, name, sub)
	case forbidden:
		f.status(w, http.StatusForbidden, "Forbidden", resource+" is forbidden")
	case broken:
		f.status(w, http.StatusInternalServerError, "InternalError", "etcdserver: request timed out")
	case q.Get("watch") == "true":
		f.watch(w, r, q, items, rv, resource, ns)
	default:
		f.list(w, q, items, rv, resource)
	}
}

// object serves GET of one object and the pods/log subresource.
func (f *API) object(w http.ResponseWriter, q url.Values, resource, ns, name, sub string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if sub == "log" {
		key := ns + "/" + name
		if q.Get("previous") == "true" {
			key += ":previous"
		}
		text, ok := f.Logs[key]
		if !ok {
			f.status(w, http.StatusNotFound, "NotFound", fmt.Sprintf("pods %q not found", name))
			return
		}
		lines := strings.SplitAfter(strings.TrimSuffix(text, "\n"), "\n")
		if text != "" {
			lines[len(lines)-1] += "\n"
		}
		if n, err := strconv.Atoi(q.Get("tailLines")); err == nil && n < len(lines) {
			lines = lines[len(lines)-n:]
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, strings.Join(lines, ""))
		return
	}
	for _, it := range f.items[resource] {
		if it.ns == ns && it.name == name {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(it.raw)
			return
		}
	}
	f.status(w, http.StatusNotFound, "NotFound", fmt.Sprintf("%s %q not found", resource, name))
}

func (f *API) list(w http.ResponseWriter, q url.Values, items []*item, rv, resource string) {
	start, _ := strconv.Atoi(q.Get("continue"))
	end := len(items)
	if l, _ := strconv.Atoi(q.Get("limit")); l > 0 && start+l < end {
		end = start + l
	}
	cont := ""
	if end < len(items) {
		cont = strconv.Itoa(end)
	}
	lk := listKinds[resource]
	var b bytes.Buffer
	fmt.Fprintf(&b, `{"kind":%q,"apiVersion":%q,"metadata":{"resourceVersion":%q,"continue":%q},"items":[`, lk[0], lk[1], rv, cont)
	for i, it := range items[start:end] {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(it.raw)
	}
	b.WriteString("]}")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b.Bytes())
}

func (f *API) watch(w http.ResponseWriter, r *http.Request, q url.Values, items []*item, rv, resource, ns string) {
	initial := q.Get("sendInitialEvents") == "true"
	f.mu.Lock()
	noWatchList := f.NoWatchList
	f.mu.Unlock()
	if initial && noWatchList {
		f.status(w, http.StatusUnprocessableEntity, "Invalid",
			"sendInitialEvents is forbidden for watch unless the WatchList feature gate is enabled")
		return
	}
	wt := &watcher{resource: resource, ns: ns, ch: make(chan []byte, 64)}
	f.mu.Lock()
	f.watchers[wt] = struct{}{}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.watchers, wt)
		f.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if initial {
		for _, it := range items {
			_, _ = w.Write([]byte(`{"type":"ADDED","object":` + string(it.raw) + "}\n"))
		}
		lk := listKinds[resource]
		fmt.Fprintf(w, `{"type":"BOOKMARK","object":{"kind":%q,"apiVersion":%q,"metadata":{"resourceVersion":%q,`+
			`"annotations":{"k8s.io/initial-events-end":"true"}}}}`+"\n", strings.TrimSuffix(lk[0], "List"), lk[1], rv)
	}
	w.(http.Flusher).Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-wt.ch:
			_, _ = w.Write(ev)
			w.(http.Flusher).Flush()
		}
	}
}
