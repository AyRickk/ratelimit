// Command upstream is envoyproxy/ratelimit's own service (src/service_cmd), built at the pinned
// commit, for the end-to-end differential tests.
package main

import (
	"github.com/envoyproxy/ratelimit/src/service_cmd/runner"
	"github.com/envoyproxy/ratelimit/src/settings"
)

func main() {
	runner := runner.NewRunner(settings.NewSettings())
	runner.Run()
}
