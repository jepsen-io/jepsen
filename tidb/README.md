# TiDB Jepsen Test

A Clojure library designed to test TiDB, a distributed NewSQL Database.

## What is being tested?

The tests run concurrent operations to some shared data from different
nodes in a TiDB cluster and checks that the operations preserve
the consistency properties defined in each test.  During the tests,
various combinations of nemeses can be added to interfere with the
database operations and exercise the database's consistency protocols.

## Running

To run a single test, try

```
lein run test --workload sets --nemesis kill --time-limit 60 --test-count 1 --concurrency 2n
```

To run the full suite, use

```
lein run test-all
```

See `lein run test --help` and `lein run test-all --help` for options.

#### Workloads

+ **append** Checks for dependency cycles in append/read transactions
+ **bank** concurrent transfers between rows of a shared table
+ **bank-multitable** multi-table variant of the bank test
+ **long-fork** distinguishes between parallel snapshot isolation and standard SI
+ **monotonic** looks for contradictory orders over increment-only registers
+ **register** concurrent atomic updates to a shared register
+ **sequential** looks for serializsble yet non-sequential orders on independent registers
+ **set** concurrent unique appends to a single table
+ **set-cas** appends elements via compare-and-set to a single row
+ **table** checks for a race condition in table creation
+ **txn-cycle** looks for write-read dependency cycles over read-write registers

#### Nemeses

+ **none** no nemesis
+ **clock-skew** randomized clock skew and strobes
+ **kill** kills random processes
+ **kill-db** kill TiDB only
+ **kill-pd** kill PD only
+ **kill-kv** kill TiKV only
+ **partition** network partitions
+ **partition-half** n/2+1 splits
+ **partition-one** isolate single nodes
+ **partition-ring** each node can see separate, intersecting majorities
+ **pause** process pauses
+ **pause-pd** pause only PD
+ **pause-kv** pause only TiKV
+ **pause-db** pause only TiDB
+ **random-merge** merge partitions
+ **restart-kv-without-pd** restart KV nodes without PD available
+ **schedules** use debugging schedules in TiDB
+ **shuffle-leader** randomly reassign TiDB leaders
+ **shuffle-region** randomly reassign TiDB regions

#### Time Limit

Time to run test, usually 60, 180, ... seconds

#### Test Count

Times to run test, should >= 1

#### Concurrency

Number of threads. 2n means "twice the number of nodes", and is a good default.

## License

Copyright © 2017--2019 TiDB, Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
