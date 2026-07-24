// Command payment-service is a synthetic payment API. It simulates payment
// processing with a configurable failure rate and reports each attempt to the
// ingestion API as a telemetry event.
package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/acbspace/sentinel-flow-project/internal/config"
	"github.com/acbspace/sentinel-flow-project/internal/demo"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, `{"level":"error","service":"payment-service","msg":"fatal","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

func run() error {
	// FAILURE_RATE overrides the default; payments fail more often than orders
	// so the demo produces a realistic mix of severities.
	cfg, err := config.LoadDemoService("payment-service", ":8083", 0.2)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	return demo.Run(cfg, demo.ServiceSpec{
		Route:         "/demo/payments",
		IDPrefix:      "pay",
		SuccessStatus: http.StatusOK,
		// A declined payment is a client-visible refusal, not a server error.
		FailureStatus: http.StatusPaymentRequired,
	})
}
