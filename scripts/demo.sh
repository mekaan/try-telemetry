#!/usr/bin/env bash
# Walks through the behaviour the challenge asks for, against the running
# service (make up). Needs curl and jq.
set -euo pipefail

API=${API:-http://localhost:8080}
CHARGER="DK-CPH-$(date +%s)"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EARLIER=$(date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-10M +%Y-%m-%dT%H:%M:%SZ)

restart() {
  docker compose restart telemetry >/dev/null 2>&1
  until curl -sf "$API/healthz" >/dev/null; do sleep 0.5; done
}
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
post() { curl -s -w '  (HTTP %{http_code})\n' -X POST "$API/v1/events" -H 'Content-Type: application/json' -d "$1"; }
status_event() { # id, time, status
  printf '{"event_id":"%s","charger_id":"%s","connector_id":1,"event_type":"status_notification","occurred_at":"%s","payload":{"status":"%s","error_code":"NoError"}}' \
    "$1" "$CHARGER" "$2" "$3"
}
state() { curl -s "$API/v1/chargers/$CHARGER/state" | jq -c "$1"; }
history_count() { curl -s "$API/v1/chargers/$CHARGER/events" | jq '.events | length'; }

step "1. Charger $CHARGER starts charging"
post "$(status_event gw-1 "$NOW" Charging)"
echo "state:   $(state '.connectors."1".status_notification.payload')"

step "2. The gateway retries the same event (duplicate)"
post "$(status_event gw-1 "$NOW" Charging)"
echo "history: $(history_count) event(s), unchanged"

step "3. An older event arrives late (out of order)"
post "$(status_event gw-0 "$EARLIER" Preparing)"
echo "state:   $(state '.connectors."1".status_notification.payload'), still Charging"
echo "history: $(history_count) events, the late one is kept"

step "4. Another team adds an event type: drop in a schema, restart, send"
if curl -s "$API/v1/event-types" | jq -e '.event_types[] | select(.event_type == "error_notification")' >/dev/null; then
  rm -rf schemas/error_notification; restart   # left over from an earlier run
fi
echo "before:  $(curl -s -X POST "$API/v1/events" -d "{\"charger_id\":\"$CHARGER\",\"connector_id\":0,\"event_type\":\"error_notification\",\"occurred_at\":\"$NOW\",\"payload\":{\"severity\":\"critical\",\"code\":\"GFCI_TRIP\"}}" | jq -r .error)"
cp -r examples/new-event-type/error_notification schemas/
trap 'rm -rf schemas/error_notification; restart' EXIT   # leave the service as we found it
restart
post "{\"charger_id\":\"$CHARGER\",\"connector_id\":0,\"event_type\":\"error_notification\",\"occurred_at\":\"$NOW\",\"payload\":{\"severity\":\"critical\",\"code\":\"GFCI_TRIP\"}}"
echo "state:   $(state '.connectors."0".error_notification.payload')"
echo "the only change was a new folder under schemas/, owned by @acme/field-ops"

step "5. A producer sends a value the contract does not allow"
post "$(status_event gw-9 "$NOW" Sleeping)"

step "Full latest state for $CHARGER"
curl -s "$API/v1/chargers/$CHARGER/state" | jq .
