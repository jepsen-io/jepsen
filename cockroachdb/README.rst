Jepsen testing for CockroachDB
==============================

CockroachDB__ is a new distributed database engine developed at
`Cockroach Labs`__.

.. __: https://github.com/cockroachdb/cockroach
.. __: http://www.cockroachlabs.com/

Jepsen__ is a testing framework for networked databases, developed by
Kyle 'Aphyr' Kingsbury to `exercise and validate the claims to
consistency made by database developers or their documentation`__.

.. __: https://github.com/aphyr/jepsen
.. __: https://aphyr.com/tags/jepsen

What is being tested?
---------------------

The tests run concurrent operations to some shared data from different
nodes in a CockroachDB cluster and checks that the operations preserve
the consistency properties defined in each test.  During the tests,
various combinations of nemeses can be added to interfere with the
database operations and exercise the database's consistency protocols.

## Running

`lein run test --test sets --nemesis subcritical-skews`

See `lein run test --help` for full options, and `full.sh` for an example. `all` is a valid value for a test or nemesis, and expands to all known tests or nemeses, respectively.

The following tests are implemented:

``register``
  concurrent atomic updates to a shared register;

``sets``
  concurrent unique appends to a shared table;

``monotonic``
  concurrent ordered appends with carried dependency;

``bank``
  concurrent transfers between rows of a shared table;

``bank-multitable``
  concurrent transfers between rows of different tables.

``g2``
  concurrent read-write cycles over a common predicate.

``sequential``
  sequential consistency over known groups of keys.

``comments``
  verifies that non-concurrent inserts are visible in order.

Nemeses:

``none``
  no nemesis

``small-skews``
  clock skews of ~100 ms

``subcritical-skews``
  clock skews of ~200 ms

``critical-skews``
  clock skews of ~250 ms

``big-skews``
  clock skews of ~500 ms

``huge-skews``
  clock skews of ~5 s

``strobe-skews``
  strobes the clock at a high frequency between 0 and 200 ms ahead

``parts``
  random network partitions

``majority-ring``
  random network partition where each node sees a majority of other nodes

``start-stop-2``
  db processes on 2 nodes are stopped and restarted with SIGSTOP/SIGCONT

``start-kill-2``
  db processes on 2 nodes are stopped with SIGKILL and restarted from scratch

``split``
  periodically splits the keyrange at a randomly selected key

``slow``
  delays network packets

Jepsen will test every combination of `nemesis` and `nemesis2`, except where
both nemeses would be identical, or both would introduce clock skew.

Test details: atomic updates
-----------------------------

One table contains a single row.

Jepsen sends concurrently to different nodes either read, write or
atomic compare-and-swap operations.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, a linearizability checker validates that the trace of
reads as observed from each client is compatible with a linearizable
history of across all nodes.

Test details: unique appends (sets)
-----------------------------------

One shared table of values.

Jepsen sends appends of unique values via different
nodes over time.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, a uniqueness checker validates that no value was
added two or more times; that all known-ok additions are indeed
present in the table; and that all known-fail additions are indeed
not present in the table.

Test details: monotonic
-----------------------

Several tables of (value, timestamp) pairs.

Jepsen sends atomic transactions that append the last known max
value + 1 and the current db's now(), concurrently to different nodes
over time.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, a monotonic checker validates that no value was added two
or more times; that all known-ok additions are indeed present in the
table; that all max values are in the same order as the now()
timestamps.

Test details: bank transfers
----------------------------

One shared table contains multiple bank accounts, one per row (or one
per table in the "bank-multitable" variant).

Jepsen sends concurrently read and transfer operations via
different nodes to/between randomly selected accounts.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, the checker validates that the sum of the remaining
balances of all accounts is the same as the initial sum.

## Test details: sequential

Cockroach does not offer strict serializability. However, as a consequence of
its implementation of hybrid logical clocks, all transactions *on a particular
node* should observe a strong real-time order. So long as CockroachDB clients
are sticky (e.g. bound to the same server), we expect those clients should
observe [sequential
consistency](https://en.wikipedia.org/wiki/Sequential_consistency) as well: the
effective order of transactions should be consistent with the order on every
client.

To verify this, we have a single client perform [a sequence of independent
transactions](https://github.com/jepsen-io/jepsen/blob/4d402bae4a216632a897be8f0795a6eff0462837/cockroachdb/src/jepsen/cockroach/sequential.clj#L76-L95),
inserting k<sub>1</sub>, k<sub>2</sub>, ..., k<sub>n</sub> into different
tables. Concurrently, a different client attempts to read each of
k<sub>n</sub>, ..., k<sub>2</sub>, k<sub>1</sub> in turn. Because all inserts
occur from the same process, they must also be visible to any single process in
that order. This implies that once a process observes k<sub>n</sub>, any
subsequent read must see k<sub>n-1</sub>, and by induction, all smaller keys.

## Test details: G2

Transactions select a predicate over two tables, then insert to one or the
other if no rows are present. Serializability implies that at most one
transaction may commit per predicate.

## Test details: comments

This test demonstrates a known strict serializability violation in Cockroach
and is intended to fail. It performs a sequence of concurrent inserts to a
table, and selects all rows from that table periodically. We look for cases
where one record was inserted strictly before another (e.g. they are not
concurrent), and the latter value is visible to a read without the former.



How to run the Jepsen tests for CockroachDB
-------------------------------------------

Overview: One computer will run the Jepsen framework, and will send
requests to other computers running the CockroachDB
database. After a while, the trace of accesses is analyzed and checked
for inconsistencies. If the database does its job properly, a Jepsen
checker will report that no inconsistencies were found;
otherwise it will indicate at which point the database started to
perform invalid operations. Optionally, some performance metrics are
also reported at the end.

How to get there:

1. ensure Sun/Oracle's JDK 8 is installed on your Jepsen host, and install leiningen__.

   .. __: http://leiningen.org/

2. configure a 5-node CockroachDB cluster, for example using the
   configuration in CockroachDB's ``cloud/aws`` subdirectory.

   .. note:: As of Jan 27th 2016, some additional tweaking may be required on
      top of the default configuration to get the database up and
      running. If in doubt, ping us on Gitter__ or our `issue tracker`__.

      .. __: https://gitter.im/cockroachdb/cockroach
      .. __: https://github.com/cockroachdb/cockroach/issues

   .. note:: If you cannot use AWS or this pre-packaged configuration,
      you can set up your cluster manually as well. The Jepsen test
      code assumes Debian Jessie on all nodes, CockroachDB available to
      run from a user account called ``ubuntu``,
      and a SSH server on each node reachable from the Jepsen
      host.

3. Ensure that all nodes can resolve each other's hostnames

4. tweak your SSH configuration on both your cluster nodes and Jepsen
   host so that you can log in password-less to the ``ubuntu`` account
   on each node from the Jepsen host.  (suggestion: create
   passwordless key pairs, populate ``authorized_keys`` where needed,
   and run ``ssh-agent`` / ``ssh-add`` on the Jespen host).

   Note: the "ubuntu" account should be able to run sudo without a password.

   (You can tweak the name of the user account in ``src/jepsen/cockroach.clj``)

6. If you are using SSL (the default), you need to:

   - ensure that your CockroachDB nodes all have their certificates set up
     to run in secure mode.

   - copy the client and CA certificates and client key to your jepsen master host.

   - generate a Java-ready encoding of the client key using the following command::

       openssl pkcs8 -topk8 -inform PEM -outform DER \
	    -in .../node.client.key -out .../node.client.pk8 -nocrypt

   - indicate the path to the client and CA certs and key in the configuration
     variables in ``src/jepsen/cockroach.clj``.

   To disable SSL instead, set ``insecure`` to false in ``src/jepsen/cockroach.clj``.

7. run ``lein run test ...`` from the ``cockroachdb`` test directory. This
   will run the Jepsen tests and exercise the database.

8. Wait for the results of the tests. There will
   be multiple reports, one per test. Each report ends with
   detailed data structure containing the test's results, including
   either ``:valid? true`` or ``:valid? false`` depending on whether
   inconsistencies were found.

9. Optionally, after the tests complete, collect additional outputs in
   the subdirectory ``cockroachdb/store/latest/``.

Browsing the test results
-------------------------

You can use Jepsen's built in web server by running `lein run serve`, or...

A small utility is provided to navigate the results using a web browser.

To use this, point a HTTP server to ``cockroachdb/store`` and
enable CGI handling for ``cockroachdb/stores/cgi-bin``; you can
also achieve this simply by running the Python embedded web server with::

  cd cockroachdb/store && python3 -m http.server --cgi 8080

Then navigate to ``/cgi-bin/display.py`` in the web browser to start
the interface.
