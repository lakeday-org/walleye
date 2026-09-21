# A medallion pipeline over live option flow

Four tiers, declared once, driven by the node. Nothing here calls refresh.

```
bronze   a worker fetches the flow on a clock and writes what arrived
silver   a JavaScript worker turns those strings into numbers and writes
         one sentence describing each print
gold     every print is labelled: direction, conviction, urgency, and how
         sure the model is about each
desk     the few worth interrupting somebody for, posted to an endpoint
```

## Running it

```sh
export UNUSUAL_WHALES_API_KEY=...     # the node holds this; no worker sees it
export TYPESAFE_API_KEY=...           # the labelling model
export WALLEYE_TOKEN=flow-demo-token-000
export WALLEYE_WORKER_FETCH_ALLOW=unusualwhales.com

walleye-node &                        # WALLEYE_ROOT_URI, WALLEYE_DIR as usual
./pipeline.sh
```

Nothing outside the node fetches anything. The script posts four view
definitions and stops.

## What each tier is

**bronze** is a worker with no source. A view with no source runs on a clock
rather than on arriving rows, and this one goes and gets its own:

```js
export default (rows, ctx) => {
  const answer = ctx.call({
    url: 'https://api.unusualwhales.com/api/option-trades/flow-alerts?limit=100',
    headers: { Authorization: 'Bearer {{env:UNUSUAL_WHALES_API_KEY}}',
               Accept: 'application/json' }
  });
  if (answer.status !== 200) throw new Error('unusual whales said ' + answer.status);
  return JSON.parse(answer.body).data.map((a) => ({ /* ...as it arrived... */ }));
}
```

Two things about that call. The host only permits a host named in
`WALLEYE_WORKER_FETCH_ALLOW`, and the default is none, so reaching out is a
decision somebody made rather than something a worker can assume. And
`{{env:...}}` is filled in by the node at the moment of the call, so the key is
not in the worker, not in the stored view definition, and not in anything a
query can read.

Premium, strike and ratios stay as text because that is how the provider sends
them. Bronze is supposed to be what actually arrived. It is keyed on the
provider's own alert id, so polling the same window twice writes nothing the
second time.

**silver** is a worker. Converting a dozen string fields to numbers and
composing a sentence is the kind of work that is tedious in SQL and ordinary in
JavaScript:

```js
export default (rows) => rows.map((r) => {
  const premium = Number(r.premium);
  const side = Number(r.ask_side_premium) > Number(r.bid_side_premium) ? 'ask' : 'bid';
  return {
    ticker: r.ticker, kind: r.kind, strike: r.strike, premium, side,
    summary: `${r.ticker} ${r.kind} $${r.strike} expiring ${r.expiry}: ` +
             `$${Math.round(premium).toLocaleString('en-US')} of premium, ` +
             `filled on the ${side} side, ...`
  };
});
```

**gold** labels each print with one call. The service answers every question
about a row in parallel, so asking three things costs about what asking one
costs. Asking them as three separate function calls would cost three times as
much, which is why this uses `decide`:

```sql
SELECT ticker, premium, summary,
       d['stance']['answer']      AS stance,
       d['stance']['confidence'] AS stance_sure,
       d['conviction']['answer']  AS conviction,
       d['urgent']['value']      AS urgency
  FROM (SELECT ticker, premium, summary, prompt_jev(summary, '<question set>') AS d
          FROM silver_flow)
```

**desk** writes nothing. It filters on confidence and posts what is left:

```json
{"source": "gold_labelled",
 "sql": "SELECT ... FROM gold_labelled WHERE stance_sure >= 0.7 AND urgency >= 0.5 AND premium >= 500000",
 "alert": {"url": "https://example.test/desk"}}
```

## What came out

From one hundred live prints:

| stance | conviction | prints |
|---|---|---|
| bullish | notable | 29 |
| bearish | aggressive | 22 |
| bearish | notable | 22 |
| neutral | notable | 14 |
| bullish | aggressive | 12 |
| bullish | routine | 1 |

Three cleared the desk threshold and were delivered:

```json
{"ticker":"SPX","stance":"bullish","conviction":"notable","premium":525600,
 "summary":"SPX put $7450 expiring 2026-09-18: $525,600 of premium, filled on the bid side, ..."}
{"ticker":"SPY","stance":"bearish","conviction":"aggressive","premium":604420,
 "summary":"SPY put $716 expiring 2026-10-16: $604,420 of premium, filled on the ask side, volume is 6.81 of open interest, ..."}
{"ticker":"IWM","stance":"bearish","conviction":"notable","premium":882504,
 "summary":"IWM put $275 expiring 2026-09-30: $882,504 of premium, filled on the ask side, ..."}
```

A put sold on the bid came back bullish and a put bought on the ask came back
bearish, which is the right way round. Index spreads came back neutral with low
confidence, which is also right: they are genuinely ambiguous, and the
confidence says so rather than guessing.

## Things worth knowing

Each tier keeps a cursor, so a second run only processes what arrived since.
Delivery happens before the cursor moves, so an endpoint that was down is
offered the same rows again rather than never.

Labelling costs one call per row. That is why it happens here, once, where the
row is written, and not in the query somebody runs later.
