#!/usr/bin/env bash
# A medallion pipeline over live option flow, labelled as it arrives.
#
#   bronze  what the provider sent, untouched
#   silver  a JavaScript worker turns provider strings into numbers and
#           writes one sentence describing each print
#   gold    every print is labelled: direction, conviction, urgency, and how
#           sure the model is about each
#   desk    the few that are worth interrupting somebody for, posted out
#
# Nothing here calls refresh. The node drives the tiers itself.
set -euo pipefail
cd "$(dirname "$0")"

: "${UNUSUAL_WHALES_API_KEY:?set UNUSUAL_WHALES_API_KEY}"
: "${TYPESAFE_API_KEY:?set TYPESAFE_API_KEY}"
export WALLEYE_TOKEN="${WALLEYE_TOKEN:-flow-demo-token}"
NODE="${WALLEYE_URL:-http://127.0.0.1:8080}"
api() { curl -fsS -H "authorization: Bearer $WALLEYE_TOKEN" -H "content-type: application/json" "$@"; }

echo "== bronze: the stream the provider writes into"
api -X POST "$NODE/v1/streams" -d '{
  "name": "bronze",
  "primary_key": ["alert_id"],
  "columns": [
    {"name":"alert_id","type":"string"},
    {"name":"ticker","type":"string"},      {"name":"kind","type":"string"},
    {"name":"strike","type":"string"},      {"name":"expiry","type":"string"},
    {"name":"premium","type":"string"},     {"name":"ask_side_premium","type":"string"},
    {"name":"bid_side_premium","type":"string"}, {"name":"volume_oi_ratio","type":"string"},
    {"name":"alert_rule","type":"string"},  {"name":"sweep","type":"string"},
    {"name":"underlying_price","type":"string"}, {"name":"observed_at","type":"string"}
  ]}' >/dev/null

echo "== silver: a worker cleans the numbers and says what happened"
api -X POST "$NODE/v1/view/silver/create/" -d @silver.json >/dev/null

echo "== gold: one call per print labels it three ways"
api -X POST "$NODE/v1/view/gold/create/" -d @gold.json >/dev/null

echo "== desk: the ones worth interrupting somebody for"
api -X POST "$NODE/v1/view/desk/create/" -d @desk.json >/dev/null

echo "== pulling live flow"
./fetch.py "${1:-25}"

echo "== waiting for the tiers to catch up (nobody is calling refresh)"
for _ in $(seq 1 90); do
  labelled=$(api -X POST "$NODE/v1/query" -d '{"sql":"SELECT count(*) AS n FROM gold_labelled"}' 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin)[0]["n"])' 2>/dev/null || echo 0)
  if [ "$labelled" -ge "${1:-25}" ]; then break; fi
  sleep 1
done

echo
echo "== the labelled tier, biggest premium first"
api -X POST "$NODE/v1/query" -d '{"sql":"SELECT ticker, stance, conviction, cast(premium AS bigint) AS premium, round(stance_sure,2) AS sure, round(urgency,2) AS urgency FROM gold_labelled ORDER BY premium DESC LIMIT 10"}' | python3 -m json.tool

echo
echo "== how the labels came out"
api -X POST "$NODE/v1/query" -d '{"sql":"SELECT stance, conviction, count(*) AS prints FROM gold_labelled GROUP BY stance, conviction ORDER BY prints DESC"}' | python3 -m json.tool
