#!/usr/bin/env bash
#
# Force an error spike so the correlation engine opens an incident.
#
# Normal demo traffic fails at ~10-20%, below the 50% error-rate threshold, so
# it never trips a rule. This posts a burst of severity=error events for one
# dedicated service straight to the ingestion API, producing a ~100% error rate
# that opens an incident within one CORRELATION_INTERVAL.
#
#   INGESTION_URL   ingestion API base URL
#   COUNT           how many error events to send (default 12)
#   SERVICE         the offending service name (default checkout-service)
#   TENANT_ID       tenant to attribute the events to (default demo-tenant)
#
# Watch the incident appear with: make incidents

set -euo pipefail

INGESTION_URL="${INGESTION_URL:-http://localhost:8080}"
COUNT="${COUNT:-12}"
SERVICE="${SERVICE:-checkout-service}"
TENANT="${TENANT_ID:-demo-tenant}"

bold=$'\033[1m'
green=$'\033[32m'
yellow=$'\033[33m'
reset=$'\033[0m'

if ! curl -fsS --max-time 5 "${INGESTION_URL}/health" >/dev/null 2>&1; then
	echo "error: ingestion-api is not reachable at ${INGESTION_URL}" >&2
	echo "start the stack first: make up" >&2
	exit 1
fi

# A portable UUID generator: uuidgen where present, the kernel source on Linux,
# and an openssl fallback (Git for Windows ships openssl) formatted canonically.
gen_uuid() {
	if command -v uuidgen >/dev/null 2>&1; then
		uuidgen
	elif [[ -r /proc/sys/kernel/random/uuid ]]; then
		cat /proc/sys/kernel/random/uuid
	else
		local h
		h=$(openssl rand -hex 16)
		printf '%s-%s-%s-%s-%s\n' "${h:0:8}" "${h:8:4}" "${h:12:4}" "${h:16:4}" "${h:20:12}"
	fi
}

echo "${bold}bursting ${COUNT} error events for ${SERVICE} (tenant ${TENANT})${reset}"

accepted=0
for ((i = 1; i <= COUNT; i++)); do
	body=$(cat <<-JSON
		{"event_id":"$(gen_uuid)","schema_version":"1.0","tenant_id":"${TENANT}","service_name":"${SERVICE}","environment":"local","event_type":"request.failed","severity":"error","timestamp":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","attributes":{"http_status_code":500}}
	JSON
	)

	status=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
		-H 'Content-Type: application/json' \
		--max-time 10 \
		-d "${body}" \
		"${INGESTION_URL}/v1/events" || echo "000")

	if [[ "${status}" == "202" ]]; then
		accepted=$((accepted + 1))
	fi
done

if [[ "${accepted}" -eq "${COUNT}" ]]; then
	echo "  ${green}accepted ${accepted}/${COUNT}${reset}"
else
	echo "  ${yellow}accepted ${accepted}/${COUNT}${reset}"
fi

echo
echo "within one CORRELATION_INTERVAL (default 15s) an incident should open:"
echo "  make incidents"
