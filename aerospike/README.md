# Aerospike

Jepsen tests for the aerospike database.

## Installation

You'll need Jepsen's build of the Java Aerospike client, which has patches to restrict clients to single nodes. You also need to have Aerospike enterprise debian 8 packages, for aerospike-server and aerospike-tools, in the `packages/` directory. For instance:

```sh
$ ls packages/
aerospike-server-enterprise-3.99.0.3.debian8.x86_64.deb
aerospike-tools-3.15.0.3.debian8.x86_64.deb
```

Jepsen will copy these packages to the remote nodes and install them there for
you.

## Usage

Demonstrate data loss with concurrent hard crashes:

```bash
lein run test --username admin --nodes-file ~/nodes --workload set -time-limit 120 --concurrency 100 --no-partitions --no-clocks --max-dead-nodes 5
```

Demonstrate data loss with sequential single-node hard crashes

```bash
lein run test --username admin --nodes-file ~/nodes --workload set -ime-limit 300 --concurrency 100 --no-partitions --no-clocks --max-dead-nodes 1 --replication-factor 2 --nemesis-interval 5 --test-count 100
```

Data loss due to process pauses

```
lein run test --username admin --nodes-file ~/nodes --workload pause --time-limit 300 --concurrency 100 --test-count 1
```

## Packages

## License

Copyright © 2015, 2017, 2018 Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
