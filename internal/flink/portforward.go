package flink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/streaming/pkg/httpstream"
)

// RESTPort is the JobManager's REST port.
const RESTPort = 8081

// Forwarder opens a local port to a pod's port and returns its base URL and a function that closes it.
type Forwarder interface {
	Forward(ctx context.Context, namespace, pod string, port int) (base string, stop func(), err error)
}

// kubeForwarder port-forwards through the API server, as kubectl port-forward does: over WebSocket, falling back
// to SPDY where a proxy or an older API server refuses the upgrade.
type kubeForwarder struct{ cfg *rest.Config }

// NewForwarder returns a Forwarder for a context's REST config.
func NewForwarder(cfg *rest.Config) Forwarder { return &kubeForwarder{cfg: cfg} }

func (f *kubeForwarder) Forward(ctx context.Context, namespace, pod string, port int) (base string, stop func(), err error) {
	u, err := url.Parse(f.cfg.Host)
	if err != nil {
		return "", nil, err
	}
	u = u.JoinPath(f.cfg.APIPath, "api/v1/namespaces", namespace, "pods", pod, "portforward")
	transport, upgrader, err := spdy.RoundTripperFor(f.cfg)
	if err != nil {
		return "", nil, err
	}
	dialer := spdy.NewDialerForStreaming(upgrader, &http.Client{Transport: transport}, http.MethodPost, u)
	if ws, err := portforward.NewSPDYOverWebsocketDialerForStreaming(u, f.cfg); err == nil {
		dialer = portforward.NewFallbackDialerForStreaming(ws, dialer, func(err error) bool {
			return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
		})
	}
	stopCh, ready := make(chan struct{}), make(chan struct{})
	pf, err := portforward.NewOnAddressesForStreaming(dialer, []string{"127.0.0.1"}, []string{"0:" + strconv.Itoa(port)}, stopCh, ready, io.Discard, io.Discard)
	if err != nil {
		return "", nil, err
	}
	failed := make(chan error, 1)
	go func() { failed <- pf.ForwardPorts() }()
	var once sync.Once
	closeFn := func() { once.Do(func() { close(stopCh) }) }
	select {
	case <-ready:
	case err := <-failed:
		return "", nil, forwardError(namespace, pod, err)
	case <-ctx.Done():
		closeFn()
		return "", nil, ctx.Err()
	}
	ports, err := pf.GetPorts()
	if err != nil || len(ports) == 0 {
		closeFn()
		return "", nil, fmt.Errorf("port-forward to %s/%s: no local port", namespace, pod)
	}
	return "http://127.0.0.1:" + strconv.Itoa(int(ports[0].Local)), closeFn, nil
}

// ErrForwardForbidden means the user may not port-forward to pods; --rest-url is the way around it.
var ErrForwardForbidden = errors.New("not allowed to port-forward to JobManager pods (needs create on pods/portforward); use --rest-url with a reachable JobManager REST endpoint")

func forwardError(namespace, pod string, err error) error {
	if err == nil {
		err = errors.New("closed before it was ready")
	}
	// The upgrade failure carries the API server's message, such as `pods "p" is forbidden: User "u" cannot
	// create resource "pods/portforward"`; a pod name that merely contains "403" must not match.
	if apierrors.IsForbidden(err) || strings.Contains(err.Error(), "is forbidden: ") {
		return ErrForwardForbidden
	}
	return fmt.Errorf("port-forward to %s/%s: %w", namespace, pod, err)
}
