# TiDB Jepsen Test

A Clojure library designed to test TiDB, a distributed NewSQL Database.

## What is being tested?

The tests run concurrent operations to some shared data from different
nodes in a CockroachDB cluster and checks that the operations preserve
the consistency properties defined in each test.  During the tests,
various combinations of nemeses can be added to interfere with the
database operations and exercise the database's consistency protocols.

## Running

`lein run test --test sets --nemesis parts --time-limit 60 --test-count 1`

*Tests*

``bank``
  concurrent transfers between rows of a shared table;

``sets``
  concurrent unique appends to a shared table;

*Nemeses*

``none``
  no nemesis

``parts``
  random network partitions

``majority-ring``
  random network partition where each node sees a majority of other nodes

``start-stop-2``
  db processes on 2 nodes are stopped and restarted with SIGSTOP/SIGCONT

``start-kill-2``
  db processes on 2 nodes are stopped with SIGKILL and restarted from scratch

*Time Limit*
time to run test, usually 60, 180, ... seconds

*Test Count*
times to run test, should >= 1

## License

Copyright © 2017 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
