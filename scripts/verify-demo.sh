#!/usr/bin/env bash
# Checks the PRD's three demo scenarios against a running demo (needs curl and jq):
#   1. end-to-end flow   2. consumer restart   3. burst of 50 orders
#
#   scripts/verify-demo.sh                 # the docker compose stack (default)
#   STOP_CONSUMER='kill $(cat consumer.pid)' START_CONSUMER='...' scripts/verify-demo.sh   # anything else
#
# Assumes the producer has not been restarted since the topic was created (its counters are
# per run), so the consumer's totals must equal what the producer says it published.
set -euo pipefail

CONSUMER=${CONSUMER_URL:-http://localhost:8080}
PRODUCER=${PRODUCER_URL:-http://localhost:8081}
STOP_CONSUMER=${STOP_CONSUMER:-docker compose stop consumer}
START_CONSUMER=${START_CONSUMER:-docker compose start consumer}
DOWNTIME=${DOWNTIME:-15}
cd "$(dirname "$0")/.."

failures=0
pass() { printf '  \033[32mPASS\033[0m %s\n' "$*"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; failures=$((failures + 1)); }
check() { local what=$1; shift; if "$@"; then pass "$what"; else fail "$what"; fi; }

state() { curl -sf "$CONSUMER/state"; }
status() { curl -sf "$PRODUCER/status"; }
now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
# The first event of an SSE stream (both services start with a snapshot).
first_event() { curl -sN --max-time 3 "$1/events" 2>/dev/null | sed -n 's/^data: //p' | head -1 || true; }

# The consumer has processed everything the producer published, and lag is 0.
caught_up() {
  local target
  target=$(status | jq .lastOffset)
  state | jq -e --argjson t "$target" '.currentOffset >= $t and .lag == 0' >/dev/null 2>&1
}

wait_caught_up() {
  local deadline=$(( $(date +%s) + ${1:-60} ))
  until caught_up; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 0.2
  done
}

# The newest $1 received orders have consecutive offsets and consecutive order ids (no gaps, no duplicates).
contiguous() {
  state | jq -e --argjson n "$1" '
    .recentOrders[:$n] as $r
    | ($r | length) == $n
      and ([range(1; $n)] | all(. as $i | $r[$i - 1].offset == $r[$i].offset + 1
                                          and $r[$i - 1].order.orderId == $r[$i].order.orderId + 1))' >/dev/null
}

totals_match() {
  local s p
  s=$(state); p=$(status)
  [ "$(jq .totalOrders <<<"$s")" = "$(jq .published <<<"$p")" ] && [ "$(jq .totalRevenue <<<"$s")" = "$(jq .revenue <<<"$p")" ]
}

echo "waiting for both services..."
for _ in $(seq 1 60); do curl -sf "$CONSUMER/health" >/dev/null && curl -sf "$PRODUCER/health" >/dev/null && break; sleep 1; done
curl -sf -X POST "$PRODUCER/resume" >/dev/null

echo "Scenario 1: end-to-end flow"
sleep 3
start=$(now_ms); target=$(status | jq .lastOffset)
until state | jq -e --argjson t "$target" '.currentOffset >= $t' >/dev/null; do sleep 0.05; done
latency=$(( $(now_ms) - start ))
check "an order the producer just published is received (${latency}ms later, budget ~1s)" [ "$latency" -le 1500 ]
sent=$(first_event "$PRODUCER" | jq -c '.recent')
received=$(state | jq -c '.recentOrders')
check "the producer's SSE stream starts with a snapshot of sent orders" [ "$(jq length <<<"$sent")" -gt 0 ]
matching=$(jq -n --argjson s "$sent" --argjson r "$received" \
  '[$s[] as $x | $r[] | select(.offset == $x.offset) | select(.order == $x.order)] | length')
check "sent and received orders match by offset and content ($matching compared)" [ "$matching" -gt 0 ]
mismatched=$(jq -n --argjson s "$sent" --argjson r "$received" \
  '[$s[] as $x | $r[] | select(.offset == $x.offset) | select(.order != $x.order)] | length')
check "no received order differs from the sent order at the same offset" [ "$mismatched" -eq 0 ]
check "the consumer's SSE stream emits order events" \
  bash -c "curl -sN --max-time 4 '$CONSUMER/events' 2>/dev/null | grep -q '^event: order'"

echo "Scenario 2: consumer restart (down for ${DOWNTIME}s)"
before=$(state | jq .currentOffset)
eval "$STOP_CONSUMER" >/dev/null 2>&1
sleep "$DOWNTIME"
published_while_down=$(( $(status | jq .lastOffset) - before ))
restart_at=$(status | jq .lastOffset)
eval "$START_CONSUMER" >/dev/null 2>&1
for _ in $(seq 1 60); do curl -sf "$CONSUMER/health" >/dev/null && break; sleep 0.5; done
check "the consumer catches up after the restart" wait_caught_up 60
curl -sf -X POST "$PRODUCER/pause" >/dev/null
wait_caught_up 30 || true
s=$(state)
resumed=$(jq .resumedFrom <<<"$s")
# `before` was read just before the stop, so one more order may have been processed and committed.
check "it resumed from its committed offset $resumed (it had processed offset $before just before stopping)" \
  [ "$resumed" -ge $((before + 1)) -a "$resumed" -le $((before + 2)) ]
check "...not from the beginning or from the latest offset ($restart_at)" [ "$resumed" -gt 0 -a "$resumed" -lt "$restart_at" ]
check "$published_while_down orders were published while it was down" [ "$published_while_down" -ge $((DOWNTIME / 2)) ]
check "the received list has no gaps or duplicates across the restart" contiguous "$(( published_while_down + 5 < 50 ? published_while_down + 5 : 50 ))"
check "lag is back to 0" [ "$(state | jq .lag)" -eq 0 ]
check "totals match what the producer published ($(state | jq -c '[.totalOrders, .totalRevenue]'))" totals_match

echo "Scenario 3: burst of 50 orders"
wait_caught_up 30 || true
curl -sf -X POST "$PRODUCER/burst?count=50" >/dev/null &
burst=$!
max_lag=0
deadline=$(( $(date +%s) + 60 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  lag=$(state | jq .lag)
  [ "$lag" -gt "$max_lag" ] && max_lag=$lag
  if ! kill -0 "$burst" 2>/dev/null && caught_up; then break; fi
  sleep 0.2
done
wait "$burst"
check "lag rose visibly during the burst (peak $max_lag)" [ "$max_lag" -gt 5 ]
check "lag drained back to 0" wait_caught_up 30
check "all 50 orders arrived in order with no gaps or duplicates" contiguous 50
check "totals match what the producer published" totals_match

curl -sf -X POST "$PRODUCER/resume" >/dev/null
echo
if [ "$failures" -eq 0 ]; then echo "all demo scenarios passed"; else echo "$failures check(s) failed"; exit 1; fi
