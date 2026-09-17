# Install

Walleye is one binary, `walleye-node`. Get one of three ways: build it, build
the container, or let somebody else run it.

## Build it

```sh
cargo build --release -p walleye-node
```

The binary lands at `target/release/walleye-node`. It needs somewhere to put
its write-ahead log, and that is the only thing it will not guess at:

```sh
export WALLEYE_ROOT_URI=file:///tmp/walleye
./target/release/walleye-node
```

That starts a node with everything on one local disk. It is the shortest way
to see the thing run, and it is not a deployment: `file://` means there is no
durability beyond that disk. Point it at a bucket when you want the real
guarantee, which is
[a single node](../self-hosting/single-node.md).

The node prints a generated token to standard error if you did not set
`WALLEYE_TOKEN`. Keep it; you need it to connect.

## Build the container

```sh
docker build -t walleye .
docker run -p 8080:8080 \
  -e WALLEYE_BUCKET=my-bucket \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION \
  walleye
```

The image's default command is `walleye-node`, and it reads the same
environment. `integration/compose.yaml` brings up a node against a local
object store if you would rather not point at a real bucket yet.

## Let somebody else run it

The managed service at [walleye.dev](https://walleye.dev) runs this binary
for you: you create an instance, and you get a URL and a token instead of a
process. Nothing below changes — the client, the protocol and the guarantees
are the same — so the rest of these pages applies either way, and says so
where it does not.

## Checking it started

```sh
curl -s localhost:8080/healthz -H "authorization: Bearer $WALLEYE_TOKEN"
curl -s localhost:8080/readyz  -H "authorization: Bearer $WALLEYE_TOKEN"
```

`/healthz` answers once the process can route. `/readyz` answers whether writes
can be made durable. On one node they say the same thing — `/readyz` is a
literal `{"ready": true}`, it names no members, and `?require=all` is ignored
rather than refused. In [a cluster](../self-hosting/cluster.md) they come apart,
which is why there are two.

## What to set next

Everything else has a default, and
[configuration](../self-hosting/configuration.md) is the whole surface. The
two worth setting before you put anything real in are `WALLEYE_RAM_GB` and
`WALLEYE_NVME_GB`: they describe the machine, not the cache, and the node
sizes everything it allocates from them. See
[budgets](../self-hosting/budgets.md).

Next: [connect](connect.md).
