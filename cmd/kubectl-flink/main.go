// Command kubectl-flink is a kubectl plugin for the Apache Flink Kubernetes Operator.
package main

import (
	"os"

	"github.com/patrk/kubectl-flink/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
