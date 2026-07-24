#!/usr/bin/env bash
#
# Drive traffic through the SentinelFlow pipeline.
#
# Each order request also calls payment-service, so one order produces two
# telemetry events travelling on two different Kafka partition keys.
#
#   ORDER_URL, PAYMENT_URL   service base URLs
#   REQUESTS                 how many of each request type to send

set -euo pipefail

ORDER_URL="${ORDER_URL:-http://localhost:8082}"
PAYMENT_URL="${PAYMENT_URL:-http://localhost:8083}"
REQUESTS="${REQUESTS:-20}"

bold=$'\033[1m'
green=$'\033[32m'
yellow=$'\033[33m'
reset=$'\033[0m'

require_service() {
	local name="$1" url="$2"
	if ! curl -fsS --max-time 5 "${url}/health" >/dev/null 2>&1; then
		echo "error: ${name} is not reachable at ${url}" >&2
		echo "start the stack first: make up" >&2
		exit 1
	fi
}

echo "${bold}SentinelFlow demo${reset}"
echo "sending ${REQUESTS} orders and ${REQUESTS} payments"
echo

require_service "order-service" "${ORDER_URL}"
require_service "payment-service" "${PAYMENT_URL}"

succeeded=0
failed=0

send() {
	local label="$1" url="$2"
	local status

	# Capture the status code separately so a simulated failure (a non-2xx
	# response, which is expected traffic here) does not abort the script.
	status=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
		-H 'Content-Type: application/json' \
		--max-time 10 \
		"${url}" || echo "000")

	if [[ "${status}" =~ ^2 ]]; then
		succeeded=$((succeeded + 1))
		printf '  %s %-8s %s%s%s\n' "${label}" "" "${green}" "${status}" "${reset}"
	else
		failed=$((failed + 1))
		printf '  %s %-8s %s%s%s\n' "${label}" "" "${yellow}" "${status}" "${reset}"
	fi
}

echo "${bold}orders${reset} (each also calls payment-service)"
for ((i = 1; i <= REQUESTS; i++)); do
	send "order  $(printf '%3d' "${i}")" "${ORDER_URL}/demo/orders"
done

echo
echo "${bold}payments${reset}"
for ((i = 1; i <= REQUESTS; i++)); do
	send "payment $(printf '%3d' "${i}")" "${PAYMENT_URL}/demo/payments"
done

echo
echo "${bold}summary${reset}"
echo "  successful responses: ${succeeded}"
echo "  failed responses:     ${failed} (simulated failures are expected)"
echo
echo "Events flow asynchronously; give the engine a moment, then run:"
echo "  make verify"
