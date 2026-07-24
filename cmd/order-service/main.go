// Command order-service is a synthetic order API. It simulates order placement,
// optionally calls payment-service, and reports the result to the ingestion API
// as a telemetry event.
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
		fmt.Fprintf(os.Stderr, `{"level":"error","service":"order-service","msg":"fatal","error":%q}`+"\n", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadDemoService("order-service", ":8082", 0.1)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	return demo.Run(cfg, demo.ServiceSpec{
		Route:         "/demo/orders",
		IDPrefix:      "ord",
		SuccessStatus: http.StatusCreated,
		FailureStatus: http.StatusInternalServerError,
		// A declined payment is the caller's problem, not a server fault.
		DownstreamFailureStatus: http.StatusPaymentRequired,
		// Set DOWNSTREAM_URL to have each order also exercise payment-service,
		// which produces a trace spanning both demo services.
		DownstreamPath: "/demo/payments",
	})
}
