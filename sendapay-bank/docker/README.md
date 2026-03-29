# Sendapay External Docker Runner

This runner keeps the Sendapay Jepsen Docker/control workflow under `sendapay-bank/docker/` instead of Jepsen's removed top-level `docker/` subtree.

It still builds the same `jepsen-control` plus `n1..n5` node topology, but the operator path is now owned by the Sendapay harness so upstream Jepsen core does not need to carry it.

For continuity with earlier local runs, it keeps the Docker Compose project name `jepsen-sendapay`.

## Quickstart

From `sendapay-bank`:

```
make docker-up
make docker-console
```

That brings up the external control/node runner and opens a shell on the Jepsen control node.

## Advanced

The underlying runner still supports direct script usage, for example:

```
JEPSEN_ROOT=/Users/mac/Desktop/repos/jepsen \
SENDAPAY_ROOT=/Users/mac/Desktop/repos/sendapay-backend \
./docker/bin/up --dev --daemon --node-count 5
```

Useful helpers:

- `make docker-console`
- `make docker-web`
- `make docker-install-jepsen`
- `make latest-nemesis-summary`
- `make latest-nemesis-summary-append-only`

Run `./docker/bin/up --help` for more runner options.
