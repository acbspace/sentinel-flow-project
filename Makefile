# SentinelFlow developer commands.
#
# Run `make help` for a summary.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Every executable lives in cmd/<name>. SERVICES are the deployable ones: they
# get built into ./bin, get a container image, and have a manifest. Keep this
# list in sync with that layout.
SERVICES := ingestion-api incident-engine incidents-api alerting remediation janitor order-service payment-service migrate

# Development-only executables in cmd/. They are deliberately absent from
# SERVICES: a load generator has no place in a deployed image, and adding one
# there would ship a traffic cannon next to the thing it points at.
TOOLS := loadgen

BIN_DIR := bin
COMPOSE := docker compose
GO := go

# Stamped into the binaries so a running process can report what it is.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags="-X main.version=$(VERSION)"

# Host-side coordinates used by the demo and integration targets.
INGESTION_URL ?= http://localhost:8080
INCIDENTS_URL ?= http://localhost:8084
ALERTING_URL ?= http://localhost:8085
REMEDIATION_URL ?= http://localhost:8086
JANITOR_URL ?= http://localhost:8087
ORDER_URL ?= http://localhost:8082
PAYMENT_URL ?= http://localhost:8083
DEMO_REQUESTS ?= 20

.PHONY: help
help: ## Show the available commands
	@echo "SentinelFlow"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---- build and verify --------------------------------------------------------

.PHONY: build
build: ## Compile every service into ./bin
	@mkdir -p $(BIN_DIR)
	@for service in $(SERVICES); do \
		echo "building $$service"; \
		CGO_ENABLED=0 $(GO) build $(LDFLAGS) -o $(BIN_DIR)/$$service ./cmd/$$service || exit 1; \
	done
	@echo "binaries written to $(BIN_DIR)/"

.PHONY: build-images
build-images: ## Build every service container image locally (for kind/minikube)
	@for service in $(SERVICES); do \
		echo "building image sentinelflow/$$service:latest"; \
		docker build --build-arg "SERVICE=$$service" -f build/Dockerfile \
			-t "sentinelflow/$$service:latest" . || exit 1; \
	done
	@echo "images built; load them into your cluster (e.g. kind load docker-image ...)"

.PHONY: k8s-apply
k8s-apply: ## Apply the Kubernetes manifests (namespace first, then the rest)
	kubectl apply -f deploy/k8s/00-namespace.yaml
	kubectl apply -f deploy/k8s/

.PHONY: k8s-delete
k8s-delete: ## Remove the SentinelFlow namespace and everything in it
	kubectl delete namespace sentinelflow --ignore-not-found

.PHONY: test
test: ## Run unit tests with the race detector
	$(GO) test -race -count=1 ./...

.PHONY: test-short
test-short: ## Run unit tests without the race detector
	$(GO) test -count=1 ./...

.PHONY: test-integration
test-integration: ## Run the end-to-end pipeline test (requires `make up`)
	POSTGRES_DSN="$${POSTGRES_DSN:-postgres://$${POSTGRES_USER:-sentinelflow}:$${POSTGRES_PASSWORD:-change-me-local-only}@localhost:$${POSTGRES_PORT:-5432}/$${POSTGRES_DB:-sentinelflow}?sslmode=disable}" \
	KAFKA_BROKERS="$${KAFKA_BROKERS:-localhost:$${KAFKA_HOST_PORT:-29092}}" \
		$(GO) test -tags=integration -count=1 -timeout=10m -v ./test/integration/...

.PHONY: cover
cover: ## Run unit tests and write an HTML coverage report
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "coverage report written to coverage.html"

.PHONY: fmt
fmt: ## Format all Go source
	$(GO) fmt ./...

.PHONY: lint
lint: ## Verify formatting and run go vet
	@echo "checking formatting"
	@unformatted=$$(gofmt -l . | grep -v '^$$' || true); \
	if [ -n "$$unformatted" ]; then \
		echo "these files are not gofmt-formatted:"; \
		echo "$$unformatted"; \
		echo "run: make fmt"; \
		exit 1; \
	fi
	@echo "running go vet"
	$(GO) vet ./...
	@echo "running go vet on tagged builds"
	$(GO) vet -tags=integration ./...
	@echo "lint passed"

.PHONY: tidy
tidy: ## Tidy and verify the module graph
	$(GO) mod tidy
	$(GO) mod verify

# ---- local environment -------------------------------------------------------

.env:
	@echo "creating .env from .env.example"
	@cp .env.example .env

.PHONY: up
up: .env ## Build the images and start the whole stack
	$(COMPOSE) up --build -d
	@echo
	@echo "waiting for services to report ready"
	@$(MAKE) --no-print-directory wait

.PHONY: wait
wait: ## Block until every application service reports ready
	@for i in $$(seq 1 90); do \
		if curl -fsS $(INGESTION_URL)/ready >/dev/null 2>&1 && \
		   curl -fsS http://localhost:8081/ready >/dev/null 2>&1 && \
		   curl -fsS $(INCIDENTS_URL)/ready >/dev/null 2>&1 && \
		   curl -fsS $(ALERTING_URL)/ready >/dev/null 2>&1 && \
		   curl -fsS $(REMEDIATION_URL)/ready >/dev/null 2>&1 && \
		   curl -fsS $(JANITOR_URL)/ready >/dev/null 2>&1; then \
			echo "stack is ready"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "services did not become ready in time; check: make logs"; \
	exit 1

.PHONY: down
down: ## Stop the stack, keeping data volumes
	$(COMPOSE) down

.PHONY: logs
logs: ## Follow logs from every service
	$(COMPOSE) logs -f --tail=100

.PHONY: ps
ps: ## Show the status of every container
	$(COMPOSE) ps

.PHONY: migrate
migrate: .env ## Apply database migrations
	$(COMPOSE) run --rm migrate up

.PHONY: migrate-down
migrate-down: .env ## Roll back the most recent migration
	$(COMPOSE) run --rm migrate down 1

# ---- demo and inspection -----------------------------------------------------

.PHONY: demo
demo: ## Send order and payment traffic through the pipeline
	@ORDER_URL=$(ORDER_URL) PAYMENT_URL=$(PAYMENT_URL) REQUESTS=$(DEMO_REQUESTS) \
		bash scripts/demo.sh

.PHONY: burst
burst: ## Force an error spike so the correlation engine opens an incident
	@INGESTION_URL=$(INGESTION_URL) bash scripts/burst.sh

.PHONY: incidents
incidents: ## Show the currently open incidents
	@curl -fsS "$(INCIDENTS_URL)/v1/incidents?status=open" \
		|| { echo "incidents-api not reachable at $(INCIDENTS_URL); is the stack up? (make up)"; exit 1; }
	@echo

.PHONY: alerts
alerts: ## Show an incident's alert timeline (usage: make alerts INCIDENT=<id>)
	@test -n "$(INCIDENT)" || { echo "usage: make alerts INCIDENT=<incident-id>   (find ids with: make incidents)"; exit 1; }
	@curl -fsS "$(INCIDENTS_URL)/v1/incidents/$(INCIDENT)/notifications" \
		|| { echo "incidents-api not reachable at $(INCIDENTS_URL); is the stack up? (make up)"; exit 1; }
	@echo

.PHONY: remediation
remediation: ## Show an incident's remediation trail (usage: make remediation INCIDENT=<id>)
	@test -n "$(INCIDENT)" || { echo "usage: make remediation INCIDENT=<incident-id>   (find ids with: make incidents)"; exit 1; }
	@curl -fsS "$(INCIDENTS_URL)/v1/incidents/$(INCIDENT)/remediation" \
		|| { echo "incidents-api not reachable at $(INCIDENTS_URL); is the stack up? (make up)"; exit 1; }
	@echo

.PHONY: approve
approve: ## Approve the pending remediation step (usage: make approve INCIDENT=<id> ACTOR=you)
	@test -n "$(INCIDENT)" || { echo "usage: make approve INCIDENT=<incident-id> [ACTOR=you]"; exit 1; }
	@curl -fsS -X POST "$(INCIDENTS_URL)/v1/incidents/$(INCIDENT)/remediation/approve?actor=$${ACTOR:-operator}" \
		|| { echo "approve failed; is a step actually awaiting a decision? (make remediation INCIDENT=$(INCIDENT))"; exit 1; }
	@echo

.PHONY: reject
reject: ## Reject the pending remediation step (usage: make reject INCIDENT=<id> ACTOR=you)
	@test -n "$(INCIDENT)" || { echo "usage: make reject INCIDENT=<incident-id> [ACTOR=you]"; exit 1; }
	@curl -fsS -X POST "$(INCIDENTS_URL)/v1/incidents/$(INCIDENT)/remediation/reject?actor=$${ACTOR:-operator}" \
		|| { echo "reject failed; is a step actually awaiting a decision? (make remediation INCIDENT=$(INCIDENT))"; exit 1; }
	@echo

.PHONY: verify
verify: ## Show what the pipeline has stored, including incidents and actions
	@bash scripts/verify.sh

# ---- benchmark ---------------------------------------------------------------

# 50 workers is the concurrency the README's table describes; the duration and
# warmup are this harness's own, since the original run's were never recorded.
# Override any of them: make bench BENCH_WORKERS=100 BENCH_DURATION=30s
BENCH_TARGETS ?= all
BENCH_WORKERS ?= 50
BENCH_DURATION ?= 10s
BENCH_WARMUP ?= 500

.PHONY: bench
bench: ## Measure the HTTP paths and print the README's benchmark table (requires `make up`)
	@echo "benchmarking against $(INGESTION_URL) and $(INCIDENTS_URL)"
	@echo "note: this writes real events; they are tagged tenant_id=bench-tenant at info severity"
	@echo
	@$(GO) run ./cmd/loadgen \
		-ingestion-url=$(INGESTION_URL) \
		-incidents-url=$(INCIDENTS_URL) \
		-targets=$(BENCH_TARGETS) \
		-workers=$(BENCH_WORKERS) \
		-duration=$(BENCH_DURATION) \
		-warmup=$(BENCH_WARMUP)

.PHONY: partitions
partitions: ## Show the telemetry_events partitions, their size and row counts
	@$(COMPOSE) exec -T postgres psql -U $${POSTGRES_USER:-sentinelflow} -d $${POSTGRES_DB:-sentinelflow} -c "\
		SELECT child.relname AS partition, \
		       pg_size_pretty(pg_total_relation_size(child.oid)) AS size, \
		       child.reltuples::bigint AS approx_rows \
		FROM pg_inherits \
		JOIN pg_class parent ON parent.oid = pg_inherits.inhparent \
		JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid \
		WHERE parent.relname = 'telemetry_events' \
		ORDER BY child.relname;"
	@echo "rows in the default partition should be 0; anything else means a day went uncreated"

.PHONY: psql
psql: ## Open a psql shell against the local database
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-sentinelflow} -d $${POSTGRES_DB:-sentinelflow}

# Git Bash on Windows rewrites arguments that look like absolute POSIX paths
# into Windows paths, which mangles container-side paths such as /opt/kafka/...
# MSYS_NO_PATHCONV disables that; it is simply ignored on Linux and macOS.
KAFKA_EXEC := MSYS_NO_PATHCONV=1 $(COMPOSE) exec -T kafka

.PHONY: topic
topic: ## Describe the telemetry Kafka topic
	@$(KAFKA_EXEC) /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe --topic $${KAFKA_TOPIC:-telemetry.events.v1}

.PHONY: lag
lag: ## Show consumer group offsets and lag
	@$(KAFKA_EXEC) /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 --describe --group $${KAFKA_CONSUMER_GROUP:-incident-engine-v1}

# ---- cleanup -----------------------------------------------------------------

.PHONY: clean
clean: ## Stop the stack, delete volumes and remove build artifacts
	$(COMPOSE) down -v --remove-orphans
	rm -rf $(BIN_DIR) coverage.out coverage.html
	@echo "cleaned"

# ---- dashboard ---------------------------------------------------------------

.PHONY: web-install
web-install: ## Install dashboard dependencies
	cd web && npm ci --no-fund --no-audit

.PHONY: web-dev
web-dev: ## Run the dashboard dev server (proxies /v1 to the incidents API)
	cd web && npm run dev

.PHONY: web-build
web-build: ## Typecheck and build the dashboard bundle
	cd web && npm run build
