#!/usr/bin/env python3
"""Pull option flow alerts and hand them to Walleye as bronze rows.

Everything arrives as the provider sends it, strings and all. Cleaning it up
is the silver tier's job, not this script's: bronze is supposed to be what
actually arrived.
"""
import json, os, sys, urllib.request

BRONZE = ["alert_id", "ticker", "kind", "strike", "expiry", "premium", "ask_side_premium",
          "bid_side_premium", "volume_oi_ratio", "alert_rule", "sweep",
          "underlying_price", "observed_at"]

def get(url, key):
    request = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {key}", "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)

def main():
    key = os.environ.get("UNUSUAL_WHALES_API_KEY")
    if not key:
        sys.exit("set UNUSUAL_WHALES_API_KEY")
    node = os.environ.get("WALLEYE_URL", "http://127.0.0.1:8080")
    token = os.environ["WALLEYE_TOKEN"]
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 25

    alerts = get(f"https://api.unusualwhales.com/api/option-trades/flow-alerts?limit={limit}", key)
    rows = [{
        "alert_id": str(a.get("id") or ""),
        "ticker": str(a.get("ticker") or ""),
        "kind": str(a.get("type") or ""),
        "strike": str(a.get("strike") or ""),
        "expiry": str(a.get("expiry") or ""),
        "premium": str(a.get("total_premium") or "0"),
        "ask_side_premium": str(a.get("total_ask_side_prem") or "0"),
        "bid_side_premium": str(a.get("total_bid_side_prem") or "0"),
        "volume_oi_ratio": str(a.get("volume_oi_ratio") or "0"),
        "alert_rule": str(a.get("alert_rule") or ""),
        "sweep": "yes" if a.get("has_sweep") else "no",
        "underlying_price": str(a.get("underlying_price") or "0"),
        "observed_at": str(a.get("created_at") or ""),
    } for a in alerts.get("data", [])]
    if not rows:
        print("no alerts returned")
        return

    body = json.dumps({"rows": rows}).encode()
    request = urllib.request.Request(
        f"{node}/v1/streams/bronze/events", data=body,
        headers={"authorization": f"Bearer {token}", "content-type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        print(f"ingested {len(rows)} alerts ->", response.read().decode())

main()
