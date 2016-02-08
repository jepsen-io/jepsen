Jepsen testing for CockroachDB
==============================

CockroachDB__ is a novel distributed database engine developed at
`Cockroach Labs`__.

.. __: https://github.com/cockroachdb/cockroach
.. __: http://www.cockroachlabs.com/

Jepsen__ is a testing framework for networked databases, developed by
Kyle 'Aphyr' Kingsbury to `exercise and validate the claims to
consistency made by database developers or their documentation`__.

.. __: https://github.com/aphyr/jepsen
.. __: https://aphyr.com/tags/jepsen

This repository is a fork of Aphyr's main Jepsen repository, with
additional tests for CockroachDB.

What is being tested?
---------------------

The tests run concurrent operations to some shared data from different
nodes in a CockroachDB cluster and checks that the data is updated
atomically and that the visible histories from all nodes are
linearizable. During the tests, random partitions are created in the
network to exercise the consistency protocol.

For now three tests are implemented:

- "atomic": concurrent atomic updates to a shared register;
- "sets":  concurrent unique appends to a shared table;
- "monotonic": concurrent unique appends with carried dependency;
- "bank": concurrent transfers between rows of a shared table. 

The database is accessed using the command-line SQL client executed
remotely via SSH on each node (``cockroach sql -e ...``).

Overview of results
-------------------

As of Jan 26 2016, following the test procedure outlined below and
the test code in this repository, during multple tests run at
Cockroach Labs there were no inconsistencies found by Jepsen.

Test details: atomic updates
-----------------------------

One table contains a single row.

Jepsen sends concurrently to different nodes either read, write or
atomic compare-and-swap operations.

Concurrently, a nemesis partitions the network between the nodes randomly.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, a linearizability checker validates that the trace of
reads as observed from each client is compatible with a linearizable
history of across all nodes.

Test details: unique appends
-----------------------------

Jepsen sends appends of unique values via different
nodes over time. 

Concurrently, a nemesis partitions the network between the nodes randomly.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, a uniqueness checker validates that no value was
added two or more times; that all known-ok additions are indeed
present in the table; and that all known-fail additions are indeed
not present in the table.

Test details: monotonic
-----------------------

Jepsen sends atomic transactions that append the last known max
value + 1, concurrently to different nodes over time.

Concurrently, a nemesis partitions the network between the nodes randomly.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, a monotonic checker validates that no value was
added two or more times; that all known-ok additions are indeed
present in the table; that all values are in order.

Test details: bank transfers
----------------------------

One shared table contains multiple bank accounts, one per row.

Jepsen sends concurrently read and transfer operations via
different nodes to/between randomly selected accounts.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, the checker validates that the sum of the remaining
balances of all accounts is the same as the initial sum.

.. note:: This test is currently incomplete. The full test should
   contain *conditional transactions*, where a transfer is only
   effected if the remaining balance in the source account is
   sufficient *within the same transaction*.  Then at the end the test
   should check that no account has a negative balance.  However this
   part of the test was not implemented because it is not possible to
   place conditionals within a transaction executed via the
   sql-over-ssh transport.

How to run the Jepsen tests for CockroachDB
-------------------------------------------

Overview: One computer will run the Jepsen framework, and will send
requests to other computers running the CockroachDB
database. After a while, the trace of accesses is analyzed and checked
for inconsistencies. If the database does its job properly, Jepsen's
checker (Knossos) will report that no inconsistencies were found;
otherwise it will indicate at which point the database started to
perform invalid operations. Optionally, some performance metrics are
also reported at the end.

How to get there:

1. ensure Sun/Oracle's JDK 8 is installed on your Jepsen host, and install leiningen__.

   .. __: http://leiningen.org/
   
2. configure a 5-node CockroachDB cluster using the configuration in
   CockroachDB's ``cloud/aws`` subdirectory. This should create 5
   Ubuntu-based VMs on EC2 with a pre-initialized, already running
   CockroachDB distributed database.

   .. note:: As of Jan 27th 2016, some additional tweaking may be required on
      top of the default configuration to get the database up and
      running. If in doubt, ping us on Gitter__ or our `issue tracker`__.

      .. __: https://gitter.im/cockroachdb/cockroach
      .. __: https://github.com/cockroachdb/cockroach/issues

   .. note:: If you cannot use AWS or this pre-packaged configuration,
      you can set up your cluster manually as well. The Jepsen test
      code assumes Ubuntu 15 on all nodes, CockroachDB running in a
      user account called ``ubuntu`` via ``supervisor``, and a SSH
      server on each node reachable from the Jepsen
      host. CockroachDB's error log is expected in
      ``/home/ubuntu/logs/cockroach.stderr``.
      
3. populate ``/etc/hosts`` on your Jepsen host machine so that the cluster nodes
   can be reached using names ``n1l`` .. ``n5l``.

4. tweak your SSH configuration on both your cluster nodes and Jepsen
   host so that you can log in password-less to the ``root`` and
   ``ubuntu`` account on each node from the Jepsen host.  (suggestion:
   create passwordless key pairs, populate ``authorized_keys`` where
   needed, and run ``ssh-agent`` / ``ssh-add`` on the Jespen host)

5. copy the two scripts ``sql.sh`` and ``restart.sh`` from the
   ``cockroachdb/scripts`` subdirectory to the directory
   ``/home/ubuntu`` on each node.

6. run ``lein test`` from the ``cockroachdb`` test directory. This
   will run the Jepsen tests and exercise the database.

7. Wait for the results of the tests. there will
   be multiple reports, one per test. Each report ends with
   detailed data structure containing the test's results, including
   either ``:valid? true`` or ``:valid? false`` depending on whether
   inconsistencies were found.

8. Optionally, after the tests complete, collect additional outputs in
   the subdirectory ``cockroachdb/stores/latest/``.


   
Example output
--------------

::

   kena@ip-172-31-50-219 ~/jepsen/cockroachdb % lein test

   lein test jepsen.cockroach-test
   INFO  jepsen.os.ubuntu - :n4l setting up ubuntu
   INFO  jepsen.os.ubuntu - :n1l setting up ubuntu
   INFO  jepsen.os.ubuntu - :n5l setting up ubuntu
   INFO  jepsen.os.ubuntu - :n3l setting up ubuntu
   INFO  jepsen.os.ubuntu - :n2l setting up ubuntu
   INFO  jepsen.cockroach - :n4l Setup complete
   INFO  jepsen.cockroach - :n3l Setup complete
   INFO  jepsen.cockroach - :n5l Setup complete
   INFO  jepsen.cockroach - :n2l Setup complete
   INFO  jepsen.cockroach - :n1l Setup complete
   INFO  jepsen.core - Worker 1 starting
   INFO  jepsen.core - Worker 3 starting
   INFO  jepsen.core - Worker 2 starting
   INFO  jepsen.core - Worker 0 starting
   INFO  jepsen.core - Worker 4 starting
   INFO  jepsen.util - 2   :invoke :cas    [0 4]
   INFO  jepsen.util - 3   :invoke :read   nil
   INFO  jepsen.util - 2   :ok     :cas    [0 4]   sql: OK OK 1 row val 0 OK 1 row val 4 OK
   INFO  jepsen.util - 0   :invoke :cas    [1 3]
   INFO  jepsen.util - 3   :ok     :read   4
   INFO  jepsen.util - 1   :invoke :write  0
   INFO  jepsen.util - 0   :fail   :cas    [1 3]   sql: OK OK 1 row val 4 OK 1 row val 4 OK
   INFO  jepsen.util - 4   :invoke :read   nil
   INFO  jepsen.util - 1   :ok     :write  0
   INFO  jepsen.util - 4   :ok     :read   0
   INFO  jepsen.util - 2   :invoke :write  3
   INFO  jepsen.util - 2   :ok     :write  3
   INFO  jepsen.util - 4   :invoke :read   nil
   INFO  jepsen.util - 4   :ok     :read   3
   INFO  jepsen.util - 2   :invoke :cas    [1 2]
   INFO  jepsen.util - 2   :fail   :cas    [1 2]   sql: OK OK 1 row val 3 OK 1 row val 3 OK
   INFO  jepsen.util - 0   :invoke :read   nil
   INFO  jepsen.util - 0   :ok     :read   3
   INFO  jepsen.util - 3   :invoke :cas    [4 1]
   INFO  jepsen.util - 3   :fail   :cas    [4 1]   sql: OK OK 1 row val 3 OK 1 row val 3 OK
   INFO  jepsen.util - 1   :invoke :write  3
   INFO  jepsen.util - 4   :invoke :cas    [3 0]
   INFO  jepsen.util - 1   :info   :write  3       sql error: OK OK OK query error: retry txn "sql/executor.go:307 sql" id=9ad30122 key=/Table/147/1/"a"/2/1 rw=true pri=0.04687035 iso=SERIALIZABLE stat
   =PENDING epo=1 ts=1453935109.322833196,1 orig=1453935109.322833196,1 max=1453935109.569188154,0
   INFO  jepsen.util - 4   :info   :cas    [3 0]   sql error: OK OK query error: read at time 1453935109.322833196,0 encountered previous write with future timestamp 1453935109.329926753,0 within uncer
   tainty interval
   INFO  jepsen.util - 3   :invoke :write  2
   [...]
   INFO  jepsen.core - Worker 0 done
   INFO  jepsen.core - Waiting for nemesis to complete
   INFO  jepsen.core - nemesis done.
   INFO  jepsen.core - Tearing down nemesis
   INFO  jepsen.core - Nemesis torn down
   INFO  jepsen.core - Snarfing log files
   INFO  jepsen.core - downloading /home/ubuntu/logs/cockroach.stderr to cockroach.stderr
   INFO  jepsen.core - downloading /home/ubuntu/logs/cockroach.stderr to cockroach.stderr
   INFO  jepsen.core - downloading /home/ubuntu/logs/cockroach.stderr to cockroach.stderr
   INFO  jepsen.core - downloading /home/ubuntu/logs/cockroach.stderr to cockroach.stderr
   INFO  jepsen.core - downloading /home/ubuntu/logs/cockroach.stderr to cockroach.stderr
   INFO  jepsen.core - Run complete, writing
   INFO  jepsen.store - Wrote /home/kena/jepsen/cockroachdb/store/cockroachdb/20160127T233103.000Z/history.txt
   INFO  jepsen.store - Wrote /home/kena/jepsen/cockroachdb/store/cockroachdb/20160127T233103.000Z/results.edn
   INFO  jepsen.core - Analyzing
   INFO  jepsen.core - Analysis complete
   INFO  jepsen.store - Wrote /home/kena/jepsen/cockroachdb/store/cockroachdb/20160127T233103.000Z/history.txt
   INFO  jepsen.store - Wrote /home/kena/jepsen/cockroachdb/store/cockroachdb/20160127T233103.000Z/results.edn
   INFO  jepsen.core - Everything looks good! ヽ(‘ー`)ノ
   {:perf
     {:latency-graph {:valid? true},
      :rate-graph {:valid? true},
      :valid? true},
    :linear
      {:valid? true,
      [...]
      },
    :valid? true}
    
   Ran 1 tests containing 1 assertions.
   0 failures, 0 errors.
   
