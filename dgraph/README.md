# Jepsen Dgraph Tests

A test suite for the Dgraph distributed graph database.

## Usage

`lein test` runs tests; use `lein test --help` for details. For instance:

```
lein run test --local-binary ~/tmp/dgraph --nemesis none --rebalance-interval 10h --sequencing server --upsert-schema --time-limit 600 --concurrency 1n --workload bank --retry-db-setup --test-count 20
```

... tests a local binary in ~/tmp/dgraph, with network partitions, automatically rebalancing every 10 hours, using server-side sequencing, @upsert schema directives, with tests lasting 600 seconds, one client per node, performing the `bank` test workload, working around database join issues in dgraph, and for 20 tests, or until one test fails.

Use `lein run test-all` to run a full test suite. Test-all takes many of the
same arguments as `test`, but cycles through all combinations of workload,
nemesis, sequencing, and upserts.

To run the full test suite:

```sh
lein run test-all --local-binary /gobin/dgraph --rebalance-interval 10h --time-limit 600 --test-count 20 --dgraph-jaeger-collector http://jaeger:14268 --tracing http://jaeger:14268/api/traces
```

To run the equivalent of `test-all` as separate `lein run test` commands in a shell loop:
```sh
TIME_LIMIT=600
TEST_COUNT=20
for i in $(seq 1 $TEST_COUNT); do
    for workload in bank delete long-fork linearizable-register uid-linearizable-register upsert set sequential; do
        if echo $workload | grep -q -E 'delete|linearizable-register$'; then
            concurrency=30
        else
            concurrency=1n
        fi
        lein run test --local-binary /gobin/dgraph --force-download --upsert-schema --rebalance-interval 10h --workload $workload --nemesis-interval 60 --nemesis move-tablet,partition-ring,kill-alpha,kill-zero,skew-clock --skew big --time-limit $TIME_LIMIT $upsert --concurrency $concurrency
    done
done
```

To run it with Jaeger trace collection:

```sh
lein run test --local-binary /gobin/dgraph --force-download --nemesis partition-ring --workload bank --rebalance-interval 10h --upsert-schema --time-limit 600 --concurrency 30 --nodes "n1, n2, n3" --replicas 3 --test-count 20 --dgraph-jaeger-collector http://jaeger:14268 --tracing http://jaeger:14268/api/traces
```

This runs a Dgraph binary at /gobin/dgraph with Jaeger running on a `jaeger` host.

To run bank tests with partition ring with 5 nodes:

```sh
lein run test --local-binary /gobin/dgraph --force-download --nemesis partition-ring --workload bank --rebalance-interval 10h --upsert-schema --time-limit 600 --concurrency 30 --replicas 3 --test-count 20 --dgraph-jaeger-collector http://jaeger:14268 --tracing http://jaeger:14268/api/traces
```

To run the skew-clock tests, use `--nemesis skew-clock` and specify the skew amount with `--skew (tiny|small|big|huge)` for 100ms, 250ms, 2s, and 7.5s clock skews respectively on random nodes:

```sh
lein run test --local-binary /gobin/dgraph --workload bank --nemesis skew-clock --skew small --rebalance-interval 10h --upsert-schema --time-limit 600 --dgraph-jaeger-collector http://jaeger:14268
```

To run the delete workload ([dgraph-io/dgraph#2391](https://github.com/dgraph-io/dgraph/issues/2391#issuecomment-391442598)):

```sh
lein run test --local-binary /gobin/dgraph --force-download --nemesis none --rebalance-interval 10h --sequencing server --upsert-schema --time-limit 600 --concurrency 30 --workload delete --retry-db-setup --test-count 20 --dgraph-jaeger-collector http://jaeger:14268
```

To run the move tablet nemesis:

```sh
lein run test --local-binary /gobin/dgraph --force-download --nemesis move-tablet --rebalance-interval 10h --time-limit 600 --concurrency 1n --nemesis move-tablet --workload bank --upsert-schema --dgraph-jaeger-collector http://jaeger:14268 --tracing http://jaeger:14268/api/traces --test-count 20
```

## License

Copyright © 2018 Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
