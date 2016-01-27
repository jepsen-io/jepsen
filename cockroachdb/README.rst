Jepsen testing for CockroachDB
==============================

CockroachDB__ is a novel distributed database engine developed at
`Cockroach Labs`__

.. __: https://github.com/cockroachdb/cockroach
.. __: http://www.cockroachlabs.com/

Jepsen__ is a testing framework for networked databases, developed by
Kyle 'Aphyr' Kingsbury to `exercise and validate the claims to
consistency made by database developers or their documentation`__.

.. __: https://github.com/aphyr/jepsen
.. __: https://aphyr.com/tags/jepsen

This repository is a fork of Aphyr's main Jepsen repository, with additional tests for CockroachDB.

What is being tested?
---------------------

The tests run concurrent operations to some shared data from different
nodes in a CockroachDB cluster and checks that the data is updated
atomically and that the visible histories from all nodes are
linearizable. During the tests, random partitions are created in the
network to exercise the consistency protocol.

For now the shared data is implemented as a single field of a single
row in a single table. The database is accessed using the command-line
SQL client executed remotely via SSH on each node (``cockroach sql -e
...``).

Overview of results
-------------------

As of Jan 26 2016, following the test procedure outlined below and
the test code in this repository, during multple tests run at
Cockroach Labs there were no inconsistencies found by Jepsen.

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

6. run ``lein test`` from the ``cockroachdb`` test directory.

7. Wait for the results of the tests. The output should end with a
   detailed data structure containing the test results, including
   either ``:valid? true`` or ``:valid? false`` depending on whether
   inconsistencies were found.

8. Optionally, after the tests complete, collect additional outputs in
   the subdirectory ``cockroachdb/stores/latest/``.


   
