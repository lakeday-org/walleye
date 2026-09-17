#!/usr/bin/env bash
# A medallion pipeline over live option flow, labelled as it arrives.
#
#   bronze  a worker fetches the flow on a clock and writes what arrived
#   silver  a JavaScript worker turns provider strings into numbers and
#           writes one sentence describing each print
#   gold    every print is labelled: direction, conviction, urgency, and how
#           sure the model is about each
#   desk    the few that are worth interrupting somebody for, posted out
#
# Nothing here calls refresh, and nothing outside the node fetches anything.
# Four view definitions, then the node runs the whole thing.
set -euo pipefail
cd "$(dirname "$0")"

: "${UNUSUAL_WHALES_API_KEY:?set UNUSUAL_WHALES_API_KEY}"
: "${TYPESAFE_API_KEY:?set TYPESAFE_API_KEY}"
export WALLEYE_TOKEN="${WALLEYE_TOKEN:-flow-demo-token}"
NODE="${WALLEYE_URL:-http://127.0.0.1:8080}"
api() { curl -fsS -H "authorization: Bearer $WALLEYE_TOKEN" -H "content-type: application/json" "$@"; }

echo "== ingest: a worker that goes and gets the flow, every minute"
api -X POST "$NODE/v1/view/ingest/create/" -d @bronze.json >/dev/null

echo "== silver: a worker cleans the numbers and says what happened"
api -X POST "$NODE/v1/view/silver/create/" -d @silver.json >/dev/null

echo "== gold: one call per print labels it three ways"
api -X POST "$NODE/v1/view/gold/create/" -d @gold.json >/dev/null

echo "== desk: the ones worth interrupting somebody for"
api -X POST "$NODE/v1/view/desk/create/" -d @desk.json >/dev/null

echo "== that is the whole declaration. waiting for the node to do the rest"
for _ in $(seq 1 120); do
  labelled=$(api -X POST "$NODE/v1/query" -d '{"sql":"SELECT count(*) AS n FROM gold_labelled"}' 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin)[0]["n"])' 2>/dev/null || echo 0)
  if [ "$labelled" -gt 0 ]; then break; fi
  sleep 2
done

echo
echo "== the labelled tier, biggest premium first"
api -X POST "$NODE/v1/query" -d '{"sql":"SELECT ticker, stance, conviction, cast(premium AS bigint) AS premium, round(stance_sure,2) AS sure, round(urgency,2) AS urgency FROM gold_labelled ORDER BY premium DESC LIMIT 10"}' | python3 -m json.tool

echo
echo "== how the labels came out"
api -X POST "$NODE/v1/query" -d '{"sql":"SELECT stance, conviction, count(*) AS prints FROM gold_labelled GROUP BY stance, conviction ORDER BY prints DESC"}' | python3 -m json.tool
