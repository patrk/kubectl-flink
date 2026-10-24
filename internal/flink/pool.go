package flink

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sync"
)

// Access runs requests against a Flink cluster's JobManager REST API.
type Access interface {
	Do(ctx context.Context, namespace, cluster string, fn func(*Client) error) error
}

// Direct is Access through a fixed REST URL, for --rest-url.
type Direct string

// Do implements Access.
func (d Direct) Do(_ context.Context, _, _ string, fn func(*Client) error) error {
	return fn(New(string(d)))
}

// JobManagers lists the names of a cluster's ready JobManager pods.
type JobManagers func(ctx context.Context, namespace, cluster string) ([]string, error)

// ErrNoJobManager means a cluster has no ready JobManager pod to forward to.
var ErrNoJobManager = errors.New("no ready JobManager pod")

// Pool keeps port-forwards to JobManagers open across requests, the most recently used first, and closes the
// least recently used beyond its size. Forwards are per cluster, so the session jobs of a session cluster share
// one.
type Pool struct {
	fwd     Forwarder
	jms     JobManagers
	size    int
	mu      sync.Mutex
	entries []*entry
}

type entry struct {
	key, pod, base string
	stop           func()
}

// NewPool returns a pool of at most size forwards.
func NewPool(fwd Forwarder, jms JobManagers, size int) *Pool {
	return &Pool{fwd: fwd, jms: jms, size: max(size, 1)}
}

// Do runs fn against the cluster's JobManager. When the forward breaks, or the JobManager answers 503 because a
// leader is being elected, the forward is dropped and fn runs once more against the next ready JobManager pod.
func (p *Pool) Do(ctx context.Context, namespace, cluster string, fn func(*Client) error) error {
	var tried []string
	for attempt := 0; ; attempt++ {
		e, err := p.get(ctx, namespace, cluster, tried)
		if err != nil {
			return err
		}
		err = fn(New(e.base))
		if attempt > 0 || !retryable(err) {
			return err
		}
		p.drop(e)
		tried = append(tried, e.pod)
	}
}

func retryable(err error) bool {
	var ue *url.Error
	var he *HTTPError
	return errors.As(err, &ue) && !ue.Timeout() || errors.As(err, &he) && he.Status == http.StatusServiceUnavailable
}

func (p *Pool) get(ctx context.Context, namespace, cluster string, exclude []string) (*entry, error) {
	key := namespace + "/" + cluster
	p.mu.Lock()
	for i, e := range p.entries {
		if e.key == key && !slices.Contains(exclude, e.pod) {
			p.entries = append([]*entry{e}, slices.Delete(p.entries, i, i+1)...)
			p.mu.Unlock()
			return e, nil
		}
	}
	p.mu.Unlock()

	pods, err := p.jms(ctx, namespace, cluster)
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("%w for %s", ErrNoJobManager, key)
	}
	// A retry prefers another JobManager; with only one, it forwards to the same pod again.
	if others := slices.DeleteFunc(slices.Clone(pods), func(pod string) bool { return slices.Contains(exclude, pod) }); len(others) > 0 {
		pods = others
	}
	base, stop, err := p.fwd.Forward(ctx, namespace, pods[0], RESTPort)
	if err != nil {
		return nil, err
	}
	e := &entry{key: key, pod: pods[0], base: base, stop: stop}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entries = append([]*entry{e}, p.entries...)
	for len(p.entries) > p.size {
		p.entries[len(p.entries)-1].stop()
		p.entries = p.entries[:len(p.entries)-1]
	}
	return e, nil
}

func (p *Pool) drop(e *entry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if i := slices.Index(p.entries, e); i >= 0 {
		p.entries = slices.Delete(p.entries, i, i+1)
	}
	e.stop()
}

// Close closes every forward.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.entries {
		e.stop()
	}
	p.entries = nil
}
