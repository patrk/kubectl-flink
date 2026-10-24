package flink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeForwarder "forwards" to an httptest server per pod and records what it opened and closed.
type fakeForwarder struct {
	mu      sync.Mutex
	servers map[string]*httptest.Server
	opened  []string
	stopped []string
	err     error
}

func (f *fakeForwarder) Forward(_ context.Context, _, pod string, _ int) (base string, stop func(), err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", nil, f.err
	}
	f.opened = append(f.opened, pod)
	srv := f.servers[pod]
	return srv.URL, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.stopped = append(f.stopped, pod)
	}, nil
}

func server(t *testing.T, status int) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"errors":["Service temporarily unavailable due to an ongoing leader election. Please refresh."],"flink-version":"2.2.1"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func jms(pods ...string) JobManagers {
	return func(context.Context, string, string) ([]string, error) { return pods, nil }
}

func getConfig(c *Client) error {
	var v map[string]any
	return c.get(context.Background(), "/config", &v)
}

func TestPoolReusesAndEvicts(t *testing.T) {
	f := &fakeForwarder{servers: map[string]*httptest.Server{}}
	for _, p := range []string{"a-jm", "b-jm", "c-jm"} {
		f.servers[p] = server(t, http.StatusOK)
	}
	pool := NewPool(f, func(_ context.Context, _, cluster string) ([]string, error) { return []string{cluster + "-jm"}, nil }, 2)
	ctx := context.Background()
	for _, cluster := range []string{"a", "b", "a", "c", "a"} {
		if err := pool.Do(ctx, "ns", cluster, getConfig); err != nil {
			t.Fatal(err)
		}
	}
	// a and b open; a reused; c evicts b (least recently used); a still open.
	if got := strings.Join(f.opened, ","); got != "a-jm,b-jm,c-jm" {
		t.Errorf("opened %s", got)
	}
	if got := strings.Join(f.stopped, ","); got != "b-jm" {
		t.Errorf("stopped %s", got)
	}
	pool.Close()
	if len(f.stopped) != 3 {
		t.Errorf("close stopped %v", f.stopped)
	}
}

func TestPoolRetriesElectionAndBrokenForward(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close() // connection refused, as through a forward whose pod is gone
	f := &fakeForwarder{servers: map[string]*httptest.Server{
		"electing": server(t, http.StatusServiceUnavailable), "leader": server(t, http.StatusOK), "gone": dead,
	}}
	ctx := context.Background()

	if err := NewPool(f, jms("electing", "leader"), 4).Do(ctx, "ns", "c", getConfig); err != nil {
		t.Errorf("electing then leader: %v", err)
	}
	if err := NewPool(f, jms("gone", "leader"), 4).Do(ctx, "ns", "c", getConfig); err != nil {
		t.Errorf("broken forward then leader: %v", err)
	}
	// Two JobManagers electing: one retry only, and the 503 comes back.
	f.servers["electing-2"] = server(t, http.StatusServiceUnavailable)
	var he *HTTPError
	if err := NewPool(f, jms("electing", "electing-2"), 4).Do(ctx, "ns", "c", getConfig); !errors.As(err, &he) || he.Status != 503 ||
		!strings.Contains(err.Error(), "leader election") {
		t.Errorf("two electing: %v", err)
	}
}

func TestPoolErrors(t *testing.T) {
	ctx := context.Background()
	if err := NewPool(&fakeForwarder{}, jms(), 4).Do(ctx, "ns", "c", getConfig); !errors.Is(err, ErrNoJobManager) {
		t.Errorf("no pods: %v", err)
	}
	f := &fakeForwarder{err: errors.New("boom")}
	if err := NewPool(f, jms("p"), 4).Do(ctx, "ns", "c", getConfig); err == nil || err.Error() != "boom" {
		t.Errorf("forward error: %v", err)
	}
	// A 404 is an answer, not a broken forward: no retry.
	f = &fakeForwarder{servers: map[string]*httptest.Server{"a": server(t, http.StatusNotFound), "b": server(t, http.StatusOK)}}
	if err := NewPool(f, jms("a", "b"), 4).Do(ctx, "ns", "c", getConfig); !IsNotFound(err) || len(f.opened) != 1 {
		t.Errorf("404: %v, opened %v", err, f.opened)
	}
	if err := Direct(f.servers["b"].URL).Do(ctx, "", "", getConfig); err != nil {
		t.Errorf("direct: %v", err)
	}
}

func TestForwardError(t *testing.T) {
	if err := forwardError("ns", "p", errors.New(`error upgrading connection: pods "p" is forbidden: User "u" cannot create resource "pods/portforward"`)); !errors.Is(err, ErrForwardForbidden) {
		t.Errorf("forbidden: %v", err)
	}
	for _, msg := range []string{"lost connection", `pods "orders-7f403b-jm" not found`} {
		if err := forwardError("ns", "p", errors.New(msg)); errors.Is(err, ErrForwardForbidden) || !strings.Contains(err.Error(), "ns/p") {
			t.Errorf("%s: %v", msg, err)
		}
	}
}
