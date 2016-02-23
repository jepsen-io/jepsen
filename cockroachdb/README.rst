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
- "monotonic-part", "monotonic-skews": concurrent ordered appends with
  carried dependency;
- "monotonic-multitable-part", "monotonic-multitable-skews": concurrent
  ordered appends to separate tables;
- "bank": concurrent transfers between rows of a shared table. 

Overview of results
-------------------

As of Jan 26 2016, following the test procedure outlined below and
the test code in this repository, during multple tests run at
Cockroach Labs there were no serious inconsistencies found by Jepsen.

- for the atomic updates, unique appends, monotonic on 1 table with
  network partition, monotonic-spread with either partitions or clock
  skews, and bank transfers: *no anomaly found*;
- for monotonic on 1 table with clock skews: some
  anomalies found when server runs *without* `--linearizable`; with
  this flag set, the anomalies disappear.

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

Test details: unique appends (sets)
-----------------------------------

One shared table of values.

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

Test details: monotonic with network partition
----------------------------------------------

One shared table of triplets (value, timestamp).

Jepsen sends atomic transactions that append the last known max
value + 1 and the current db's now(), concurrently to different nodes
over time.

Concurrently, a nemesis partitions the network between the nodes randomly.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, a monotonic checker validates that no value was added two
or more times; that all known-ok additions are indeed present in the
table; that all max values are in the same order as the now()
timestamps.

Test details: monotonic with clock skews
----------------------------------------

Same test setup as above; however here the nemesis insert random clock
skews of +/- 100ms on each node instead of partitions.

Test details: monotonic over multiple tables, network partitions
----------------------------------------------------------------

Multiple (e.g. 5) tables, each containing triplets (value, timestamp, clientid).

Each client repeatedly:
- picks one of the random tables,
- inserts the value of a local (per client) counter, the db timestamp
  and its own client ID in the randomly chosen table,
- records either success for the added value, or failure.

Concurrently, a nemesis partitions the network between the nodes randomly.

Each node may report ok, the operation is known to have succeeded;
fail, the operation is known to have failed; and unknown otherwise
(e.g. the connection dropped before the answer could be read).

At the end, a monotonic checker validates that no value was added two
or more times; that all known-ok additions are indeed present in some
table; and that, per client id, the merged history for that client id
across all tables presents the client's counter value in the same
order as the db timestamp.

Test details: monotonic over multiple tables, clock skews
----------------------------------------------------------------

Same test setup as above; however here the nemesis insert random clock
skews of +/- 100ms on each node instead of partitions.

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
   
2. configure a 5-node CockroachDB cluster, for example using the
   configuration in CockroachDB's ``cloud/aws`` subdirectory. This
   should create 5 Ubuntu-based VMs on EC2 with a pre-initialized,
   already running CockroachDB distributed database.

   .. note:: As of Jan 27th 2016, some additional tweaking may be required on
      top of the default configuration to get the database up and
      running. If in doubt, ping us on Gitter__ or our `issue tracker`__.

      .. __: https://gitter.im/cockroachdb/cockroach
      .. __: https://github.com/cockroachdb/cockroach/issues

   .. note:: If you cannot use AWS or this pre-packaged configuration,
      you can set up your cluster manually as well. The Jepsen test
      code assumes Ubuntu 15 on all nodes, CockroachDB available to
      run from a user account called ``ubuntu``,
      and a SSH server on each node reachable from the Jepsen
      host. 
      
3. populate ``/etc/hosts`` on your Jepsen host machine so that the cluster nodes
   can be reached using names ``n1l`` .. ``n5l``.

4. tweak your SSH configuration on both your cluster nodes and Jepsen
   host so that you can log in password-less to the ``ubuntu`` account
   on each node from the Jepsen host.  (suggestion: create
   passwordless key pairs, populate ``authorized_keys`` where needed,
   and run ``ssh-agent`` / ``ssh-add`` on the Jespen host).

   Note: the "ubuntu" account should be able to run sudo without a password.

   (You can tweak the name of the user account in ``src/jepsen/cockroach.clj``)

5. Compile ``cockroachdb/scripts/adjtime.c`` and copy it to
   ``/home/ubuntu`` on each node.

   (You can tweak the location of this program in ``src/jepsen/cockroach.clj``)

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
   
7. run ``lein test`` from the ``cockroachdb`` test directory. This
   will run the Jepsen tests and exercise the database.

8. Wait for the results of the tests. there will
   be multiple reports, one per test. Each report ends with
   detailed data structure containing the test's results, including
   either ``:valid? true`` or ``:valid? false`` depending on whether
   inconsistencies were found.

9. Optionally, after the tests complete, collect additional outputs in
   the subdirectory ``cockroachdb/store/latest/``.

Browsing the test results
-------------------------

A small utility is provided to navigate the results using a web browser.

To use this, point a HTTP server to ``cockroachdb/store`` and
enable CGI handling for ``cockroachdb/stores/cgi-bin``; you can
also achieve this simply by running the Python embedded web server with::

  cd cockroachdb/store && python3 -m http.server --cgi 8080

Then navigate to ``/cgi-bin/display.py`` in the web browser to start
the interface.
