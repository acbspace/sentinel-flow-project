#!/usr/bin/env bash
#
# Show what the pipeline has actually stored in PostgreSQL.

set -euo pipefail

COMPOSE="${COMPOSE:-docker compose}"
PGUSER_NAME="${POSTGRES_USER:-sentinelflow}"
PGDATABASE_NAME="${POSTGRES_DB:-sentinelflow}"

run_sql() {
	${COMPOSE} exec -T postgres psql -U "${PGUSER_NAME}" -d "${PGDATABASE_NAME}" -X -q -c "$1"
}

bold=$'\033[1m'
reset=$'\033[0m'

echo "${bold}Total stored events${reset}"
run_sql "SELECT count(*) AS total_events FROM telemetry_events;"

echo "${bold}Events by service and severity${reset}"
run_sql "SELECT service_name, severity, count(*) AS events
         FROM telemetry_events
         GROUP BY service_name, severity
         ORDER BY service_name, severity;"

echo "${bold}Distinct event IDs versus rows (these must match)${reset}"
run_sql "SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_event_ids
         FROM telemetry_events;"

echo "${bold}End-to-end pipeline latency${reset}"
run_sql "SELECT
           round(avg(extract(epoch FROM (processed_at - received_at)) * 1000)::numeric, 2) AS avg_ms,
           round(max(extract(epoch FROM (processed_at - received_at)) * 1000)::numeric, 2) AS max_ms
         FROM telemetry_events;"

echo "${bold}10 most recent events${reset}"
run_sql "SELECT
           left(event_id::text, 8) AS event_id,
           service_name,
           severity,
           attributes->>'http_status_code' AS status,
           attributes->>'latency_ms' AS latency_ms,
           to_char(event_timestamp, 'HH24:MI:SS') AS at
         FROM telemetry_events
         ORDER BY event_timestamp DESC
         LIMIT 10;"

echo "${bold}Incidents by status and severity${reset}"
run_sql "SELECT status, severity, count(*) AS incidents
         FROM incidents
         GROUP BY status, severity
         ORDER BY status, severity;"

echo "${bold}10 most recent incidents${reset}"
run_sql "SELECT
           left(id::text, 8) AS id,
           service_name,
           severity,
           status,
           event_count,
           to_char(last_seen_at, 'HH24:MI:SS') AS last_seen
         FROM incidents
         ORDER BY last_seen_at DESC
         LIMIT 10;"

echo "${bold}Remediation actions by status${reset}"
run_sql "SELECT runbook_id, status, count(*) AS actions
         FROM remediation_actions
         GROUP BY runbook_id, status
         ORDER BY runbook_id, status;"

echo "${bold}10 most recent remediation actions${reset}"
run_sql "SELECT
           left(incident_id::text, 8) AS incident,
           step_index AS step,
           step_name,
           mode,
           status,
           actor,
           to_char(updated_at, 'HH24:MI:SS') AS updated
         FROM remediation_actions
         ORDER BY updated_at DESC
         LIMIT 10;"

echo "${bold}10 most recent alert notifications${reset}"
run_sql "SELECT
           left(incident_id::text, 8) AS incident,
           level,
           target,
           contact,
           channel,
           status,
           to_char(sent_at, 'HH24:MI:SS') AS sent
         FROM notifications
         ORDER BY sent_at DESC
         LIMIT 10;"
