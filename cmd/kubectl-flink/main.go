// Command kubectl-flink is a kubectl plugin for the Apache Flink Kubernetes Operator.
package main

import (
	"os"

	// The kubeconfig auth providers client-go still has in-tree (OIDC); exec plugins such as the EKS, AKS and GKE
	// ones need nothing.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/patrk/kubectl-flink/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
