# Sendapay Jepsen Bank Harness

This Jepsen harness exercises Sendapay's P2P transfer confirm path in either:

1. a local control-node mode against the host Postgres, or
2. a Docker topology with `jepsen-control` driving Sendapay app/helper nodes on `n1` and `n3`, plus Postgres on `n2`.

It does not modify the Sendapay repo. Instead, it:

1. rebuilds a dedicated benchmark DB,
2. provisions a fixed set of wallet users through Sendapay's internal services,
3. runs Jepsen's `bank`-style workload against Sendapay's real `quote_transfer_intent` + `confirm_transfer_intent` functions,
4. reads authoritative ledger balances for the tracked wallets plus the fee and issuance system wallets.

That last point matters because Sendapay transfers charge fees. A pure "sum of user wallets" model would report false money loss; the tracked read set includes the fee wallet and issuance wallet so the total remains meaningful.

The helper now reads balances under one repeatable-read snapshot. The earlier `wrong-total` failures in the wide runs were torn reads across individual wallets, not money disappearing into an untracked hold-only state.

## Local Prereqs

- Sendapay backend repo at `/Users/mac/Desktop/repos/sendapay-backend`
- Sendapay virtualenv at `/Users/mac/Desktop/repos/sendapay-backend/.venv`
- Sendapay Docker Postgres reachable on `127.0.0.1:15432`
- Local Jepsen library installed into `~/.m2`

Install the Jepsen library once:

```bash
cd /Users/mac/Desktop/repos/jepsen/jepsen
lein install
```

## Local Run

```bash
cd /Users/mac/Desktop/repos/jepsen/sendapay-bank
lein run -- \
  --sendapay-root /Users/mac/Desktop/repos/sendapay-backend \
  --db-url postgresql+psycopg://sendapay:sendapay@127.0.0.1:15432/sendapay_jepsen_bank?connect_timeout=5 \
  --account-count 8 \
  --initial-balance-cents 100000 \
  --max-transfer-cents 2500 \
  --concurrency 4 \
  --time-limit 15
```

Optional:

- `--append-only` toggles the Sendapay append-only confirm flags used by the existing benchmark lanes.
- `--state-file /abs/path.json` keeps the generated helper state somewhere explicit.

Artifacts land in `store/sendapay-bank-classic/...` or `store/sendapay-bank-append-only/...`.

For Docker runs, each test directory now also captures:

- `artifacts/app/<node>/helper.log` for each Sendapay app/helper node
- `artifacts/app/<node>/nemesis/*.tsv` for transition-time helper summaries, including recent command counts, HTTP status counts, queue-unavailable events, connection/OperationalError counts, and helper process counts
- `artifacts/app/<node>/nemesis/*.log` for the recent helper-log tail captured at each transition
- `artifacts/db/<db-node>/postgresql.log` from the Postgres node
- `artifacts/db/<db-node>/postgresql-journal.log` from `journalctl -u postgresql`
- `artifacts/db/<db-node>/pg-stat-activity.tsv` for a tab-separated `pg_stat_activity` snapshot
- `artifacts/db/<db-node>/pg-locks.tsv` for a tab-separated `pg_locks` snapshot
- `artifacts/db/<db-node>/nemesis/*.tsv` for banner-free transition-time snapshots captured when the nemesis starts or heals a fault, including machine-parsable error rows if PostgreSQL is unavailable during capture
- `artifacts/state/sendapay-bank-state.json` so the tracked wallet metadata used by the run is preserved with the checker outputs

`results.edn` now also includes a `:nemesis-summary` checker section. It summarizes the parsed transition snapshots with max blocked/waiting session counts, max lock counts, and the helper-side error/HTTP/process counters, so you can inspect fault evidence without preprocessing the raw `.tsv` artifacts.

## Docker Topology

Topology choice:

- `control`: Jepsen runner and artifact store
- `n1`, `n3`: Sendapay helper/app nodes
- `n2`: PostgreSQL node
- `n4`, `n5`: spare nodes for future fanout or broader nemesis work

The control container needs the Sendapay backend repo mounted. `docker/docker-compose.dev.yml` now mounts `${SENDAPAY_ROOT:-/Users/mac/Desktop/repos/sendapay-backend}` at `/workspace/sendapay-backend`.

`docker/bin/up` now generates an RSA PEM key for the control lane, and `docker/control/init.sh` also derives `/root/.ssh/id_rsa.pem` on startup so SSHJ can authenticate directly on arm64 without falling into the old agentproxy `jnidispatch` crash.

Bring the cluster up or recreate control after changing the mount:

```bash
export JEPSEN_ROOT=/Users/mac/Desktop/repos/jepsen
export SENDAPAY_ROOT=/Users/mac/Desktop/repos/sendapay-backend
cd /Users/mac/Desktop/repos/jepsen/docker
docker compose --compatibility -p jepsen-sendapay -f docker-compose.yml -f docker-compose.dev.yml up -d --build --force-recreate control
```

If you change `/Users/mac/Desktop/repos/jepsen/jepsen` while using the dev mount, reinstall the local Jepsen library inside the control container before rerunning the harness:

```bash
docker exec jepsen-control bash -lc 'cd /jepsen/jepsen && lein install'
```

Run the harness inside `jepsen-control`:

```bash
cd /Users/mac/Desktop/repos/jepsen/sendapay-bank
make docker-smoke
```

Useful variants:

- `make docker-smoke-append-only`
- `make docker-partition-smoke`
- `make docker-partition-smoke-append-only`
- `make docker-restart-smoke`
- `make docker-restart-smoke-append-only`
- `make docker-stress`
- `make docker-stress-append-only`
- `make docker-partition-stress`
- `make docker-partition-stress-append-only`
- `make docker-restart-stress`
- `make docker-restart-stress-append-only`
- `make docker-fault-stress`
- `make docker-fault-stress-append-only`

The underlying `make docker-run` target accepts overrides, for example:

```bash
make docker-run \
  ACCOUNT_COUNT=24 \
  CONCURRENCY=12 \
  TIME_LIMIT=90 \
  NEMESIS=partition-app-db
```

For quicker wrapper verification, `docker-fault-stress` also accepts the shared fault-envelope knobs:

```bash
make docker-fault-stress-append-only \
  FAULT_STRESS_ACCOUNT_COUNT=3 \
  FAULT_STRESS_INITIAL_BALANCE_CENTS=10000 \
  FAULT_STRESS_MAX_TRANSFER_CENTS=200 \
  FAULT_STRESS_CONCURRENCY=4 \
  FAULT_STRESS_TIME_LIMIT=20
```

Docker mode stages the backend repo and helper into `/var/jepsen/shared`, provisions Postgres on `n2`, prepares the Python/runtime dependencies on `n1` and `n3`, and spreads Jepsen client workers across those app nodes. The helper state file lives on the shared volume, so both app nodes operate on the same tracked-account metadata.

`--nemesis` currently supports:

- `none`: no fault injection
- `partition-app-db`: partitions all app nodes away from `n2`
- `restart-db`: stops PostgreSQL on `n2` and starts it again on recovery
