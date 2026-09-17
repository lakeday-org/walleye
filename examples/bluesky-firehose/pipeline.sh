#!/usr/bin/env bash
# What is Bluesky talking about, right now.
#
#   posts     a socket the node holds open, handing frames to a worker that
#             keeps the post creates and throws the rest away
#   sampled   one post in forty, English, long enough to be about something
#   labelled  topic, heat, and whether a news desk would want it, per post
#   desk      the few worth someone looking at, posted out
#
# The firehose needs no account and no key. Nothing here calls refresh and
# nothing outside the node connects to anything.
set -euo pipefail
cd "$(dirname "$0")"

export WALLEYE_TOKEN="${WALLEYE_TOKEN:-firehose-demo-token}"
NODE="${WALLEYE_URL:-http://127.0.0.1:8080}"
api() { curl -fsS -H "authorization: Bearer $WALLEYE_TOKEN" -H "content-type: application/json" "$@"; }

echo "== posts: the socket, and a worker that reads its frames"
api -X POST "$NODE/v1/view/firehose/create/" -d @bronze.json >/dev/null

echo "== sampled: one in forty, deterministically"
api -X POST "$NODE/v1/view/sampled/create/" -d @silver.json >/dev/null

echo "== labelled: one call per post, three questions at once"
api -X POST "$NODE/v1/view/labelled/create/" -d @gold.json >/dev/null

echo "== desk: what a news desk would want"
api -X POST "$NODE/v1/view/desk/create/" -d @desk.json >/dev/null

echo "== that is the whole declaration. watching it fill"
for _ in $(seq 1 90); do
  n=$(api -X POST "$NODE/v1/query" -d '{"sql":"SELECT count(*) AS n FROM labelled"}' 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin)[0]["n"])' 2>/dev/null || echo 0)
  printf "\r   posts labelled: %s " "$n"
  [ "$n" -ge "${1:-40}" ] && break
  sleep 2
done
echo; echo

echo "== what it is talking about"
api -X POST "$NODE/v1/query" -d '{"sql":"SELECT topic, count(*) AS posts, round(avg(newsworthy),2) AS avg_newsworthy FROM labelled GROUP BY topic ORDER BY posts DESC"}' | python3 -m json.tool

echo
echo "== how heated"
api -X POST "$NODE/v1/query" -d '{"sql":"SELECT heat, count(*) AS posts FROM labelled GROUP BY heat ORDER BY posts DESC"}' | python3 -m json.tool

echo
echo "== the ones a desk would look at"
api -X POST "$NODE/v1/query" -d '{"sql":"SELECT topic, heat, round(newsworthy,2) AS newsworthy, substr(text, 1, 120) AS text FROM labelled WHERE newsworthy >= 0.75 ORDER BY newsworthy DESC LIMIT 8"}' | python3 -m json.tool
