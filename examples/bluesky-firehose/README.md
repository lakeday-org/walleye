# What Bluesky is talking about, right now

A socket the node holds open, a worker reading its frames, and every sampled
post labelled as it arrives. No account, no key, no script.

```
posts     the firehose, kept down to post creates with text in them
sampled   one post in forty, English, long enough to be about something
labelled  topic, heat, and whether a news desk would want it
desk      the few worth someone looking at, posted out
```

## Running it

```sh
export TYPESAFE_API_KEY=...            # the labelling model
export WALLEYE_TOKEN=firehose-demo-token

walleye-node &                         # WALLEYE_ROOT_URI, WALLEYE_DIR as usual
./pipeline.sh
```

The firehose itself needs no credentials. The script posts four view
definitions and then only reads.

## The socket

A view names a socket and the node keeps it connected:

```json
{
  "websocket": {
    "url": "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    "frames": 400,
    "window_ms": 2000
  },
  "target": "posts",
  "worker": "..."
}
```

The connection lives in the node, not in the isolate. Frames are gathered
until there are four hundred of them or two seconds have passed, whichever
comes first, and that batch is one worker turn. So the worker stays a bounded,
stoppable thing while the stream itself keeps running, and a batch that fails
costs itself rather than the connection.

The worker throws most of the firehose away, which is what bronze is for:

```js
export default (rows) => rows.flatMap((r) => {
  let event;
  try { event = JSON.parse(r.data); } catch { return []; }
  const commit = event.commit;
  if (!commit || commit.operation !== 'create') return [];
  if (commit.collection !== 'app.bsky.feed.post') return [];
  const record = commit.record || {};
  const text = typeof record.text === 'string' ? record.text.trim() : '';
  if (!text) return [];
  return [{ uri: event.did + '/' + commit.rkey, author: event.did,
            lang: (record.langs && record.langs[0]) || '',
            text, posted_at: record.createdAt || '',
            at_us: String(event.time_us || '') }];
})
```

## Sampling, and why it is deterministic

Labelling costs one call per post and the firehose does thousands a minute, so
the second tier takes a fixed share. It buckets on the post's own identity
rather than rolling dice:

```js
const ONE_IN = 40;
function bucket(text) {
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) % ONE_IN;
}
export default (rows) => rows
  .filter((r) => r.lang === 'en' && r.text.length >= 80 && bucket(r.uri) === 0)
  .map((r) => ({ uri: r.uri, author: r.author, posted_at: r.posted_at,
                 text: r.text.length > 400 ? r.text.slice(0, 400) : r.text }))
```

The same post is always kept or always dropped. That matters because a view
replays a batch it did not finish, and a random sample would produce different
rows the second time, which would break the collapse that makes a replay
harmless.

## Labelling

Three questions, one call per post:

```sql
SELECT uri, author, posted_at, text,
       d['topic']['label']      AS topic,
       d['topic']['confidence'] AS topic_sure,
       d['heat']['label']       AS heat,
       d['newsworthy']['value'] AS newsworthy
  FROM (SELECT uri, author, posted_at, text,
               decide(text, '<question set>') AS d FROM sampled)
```

Topic is a choice across news, technology, culture, sport, personal and other.
Heat is a rubric from calm through opinionated to angry, judged on how
something is written rather than what it is about. Newsworthy is a yes or no
proposition about whether a desk would want to look into it.

## What came out

A few minutes of the live firehose:

| tier | rows |
|---|---|
| posts | 7598 |
| sampled | 46 |
| labelled | 46 |

| topic | posts |
|---|---|
| culture | 19 |
| personal | 12 |
| news | 8 |
| sport | 3 |
| technology | 2 |
| other | 2 |

| heat | posts |
|---|---|
| opinionated | 21 |
| calm | 21 |
| angry | 4 |

The labels hold up on inspection. A high school volleyball scoreboard came
back as sport with confidence 1.0. A transcribed 1909 court report came back
as news, calm, and only middling on newsworthy, which is the right reading of
a history account. What the desk actually received:

```json
{"topic":"news","heat":"calm","newsworthy":0.59,
 "text":"Turkic states prepare for Ankara summit, Europol-style agency to be discussed ... Policing, digital trade and education are moving at different speeds ahead of the 30 October summit"}
```

## The threshold is the interesting part

Most posts score low on newsworthy, and they should: most of a social firehose
is not news. An earlier version of this example asked for 0.75 and the desk
stayed empty for the whole run. That is the model being calibrated rather than
eager, and it means the threshold is a decision somebody makes with the
numbers in front of them, not a constant to guess at.

## Things worth knowing

Each tier keeps a cursor, so the pipeline resumes rather than restarts.
Reconnection backs off to a minute, and dropping the view takes the socket
with it.

The firehose is public data, and this example only counts and labels it.
Anything that stores or acts on individual posts is a decision with people on
the other end of it.
