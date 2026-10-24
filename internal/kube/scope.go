package kube

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Scope is where to look: a context and its namespaces.
type Scope struct {
	Context string `json:"context"`
	// Namespaces to read; empty means the context's namespace, and so does "" among others.
	Namespaces []string `json:"namespaces,omitempty"`
	// AllNamespaces asks for cluster scope, falling back to Namespaces when that is forbidden.
	AllNamespaces bool `json:"allNamespaces,omitempty"`
}

// Plan is a scope resolved against what the user may list.
type Plan struct {
	Scope
	// Read lists the namespaces to read; a single "" means cluster scope.
	Read []string
	// Note explains a fallback in one line; empty when the request was met as asked.
	Note string
}

// ErrNoOperator means the Flink operator's resources are not installed in the context.
var ErrNoOperator = errors.New("the Flink operator's resources are not installed")

// Resolve decides which namespaces to read. Cluster scope is probed with a one-item LIST before anything is
// loaded, because a Forbidden from a running informer surfaces too late to re-plan. When it is forbidden, the
// scope's namespaces are used, then the context namespace.
func (c *Client) Resolve(ctx context.Context, s Scope, contextNamespace string) (Plan, error) {
	p := Plan{Scope: s}
	var fallback []string
	for _, ns := range s.Namespaces {
		if ns == "" {
			ns = contextNamespace
		}
		if !slices.Contains(fallback, ns) {
			fallback = append(fallback, ns)
		}
	}
	if len(fallback) == 0 {
		fallback = []string{contextNamespace}
	}
	if !s.AllNamespaces {
		p.Read = fallback
		return p, nil
	}
	// Both kinds are probed: RBAC may allow one at cluster scope and not the other.
	err := c.Probe(ctx, Deployments, "")
	if err == nil {
		err = c.Probe(ctx, SessionJobs, "")
	}
	switch {
	case err == nil:
		p.Read = []string{""}
	case apierrors.IsForbidden(err):
		p.Read = fallback
		p.Note = fmt.Sprintf("cannot list Flink resources at cluster scope in context %s; showing namespace %s",
			s.Context, strings.Join(fallback, ", "))
	case apierrors.IsNotFound(err):
		return p, fmt.Errorf("context %s: %w", s.Context, ErrNoOperator)
	default:
		return p, fmt.Errorf("context %s: %w", s.Context, err)
	}
	return p, nil
}
