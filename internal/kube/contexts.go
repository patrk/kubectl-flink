package kube

import (
	"fmt"
	"slices"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// Contexts builds REST configs for kubeconfig contexts. The selected context gets every kubectl flag
// (--server, --token, --as and so on); other contexts named by a profile get only the kubeconfig and its
// loading rules, so --kubeconfig applies to all of them.
type Contexts struct {
	flags   *genericclioptions.ConfigFlags
	raw     clientcmdapi.Config
	current string
}

// NewContexts reads the kubeconfig the flags point at.
func NewContexts(flags *genericclioptions.ConfigFlags) (*Contexts, error) {
	raw, err := flags.ToRawKubeConfigLoader().RawConfig()
	if err != nil {
		return nil, fmt.Errorf("read kubeconfig: %w", err)
	}
	cur := raw.CurrentContext
	if flags.Context != nil && *flags.Context != "" {
		cur = *flags.Context
	}
	return &Contexts{flags: flags, raw: raw, current: cur}, nil
}

// Current is the context of --context, or the kubeconfig's current context.
func (c *Contexts) Current() string { return c.current }

// Config returns the REST config and default namespace of a context; "" means the current one.
func (c *Contexts) Config(name string) (*rest.Config, string, error) {
	if name == "" || name == c.current {
		cfg, err := c.flags.ToRESTConfig()
		if err != nil {
			return nil, "", fmt.Errorf("context %s: %w", c.current, err)
		}
		ns, _, err := c.flags.ToRawKubeConfigLoader().Namespace()
		return cfg, ns, err
	}
	if _, ok := c.raw.Contexts[name]; !ok {
		return nil, "", fmt.Errorf("context %s not found in kubeconfig", name)
	}
	cc := clientcmd.NewNonInteractiveClientConfig(c.raw, name, &clientcmd.ConfigOverrides{}, nil)
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("context %s: %w", name, err)
	}
	ns, _, err := cc.Namespace()
	return cfg, ns, err
}

// Names lists the kubeconfig's contexts, sorted.
func (c *Contexts) Names() []string {
	out := make([]string, 0, len(c.raw.Contexts))
	for name := range c.raw.Contexts {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}
