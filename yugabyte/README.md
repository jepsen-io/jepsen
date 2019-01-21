# Jepsen tests for YugaByteDB

[YugaByteDB](https://github.com/YugaByte/yugabyte-db) is a transactional, high-performance database for building distributed cloud services developed by [YugaByte](http://www.yugabyte.com).

[Jepsen](https://github.com/aphyr/jepsen) is a testing framework for networked
databases, developed by Kyle 'Aphyr' Kingsbury to exercise and
[validate](https://jepsen.io) the claims to consistency made by database
developers or their documentation.

## What is being tested?

The tests run concurrent operations to some shared data from different nodes in a YugaByteDB cluster and checks that
the operations preserve the consistency properties defined in each test. During the tests, various combinations of
nemeses can be added to interfere with the database operations and exercise the database's consistency protocols.

## Running

### Prerequisites for CentOS 7

Install [https://leiningen.org](https://leiningen.org):
```bash
mkdir ~/bin
curl https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein -o /home/centos/bin/lein \
  && chmod +x ~/bin/lein
```

Add `~/bin` to `$PATH`. 

Install Cassaforte driver:
```bash
mkdir ~/code
git clone https://github.com/YugaByte/cassaforte ~/code/cassaforte
cd ~/code/cassaforte
git checkout driver-3.0-yb
lein install
```
Install Knossos library:
```bash
git clone https://github.com/YugaByte/knossos ~/code/knossos
cd ~/code/knossos
lein install
```
Install Jepsen framework:
```bash
git clone https://github.com/YugaByte/jepsen ~/code/jepsen
cd ~/code/jepsen/jepsen
lein install
```
Install gnuplot:
```bash
sudo yum install gnuplot
```

### YugaByteDB cluster setup

- Create YugaByteDB cluster with 5 nodes and replication factor of 3.
- Create text file `~/code/jepsen/nodes` and list all cluster nodes there - one per line, for example:
```bash
yb-test-jepsen-n1
yb-test-jepsen-n2
yb-test-jepsen-n3
yb-test-jepsen-n4
yb-test-jepsen-n5
```
- Setup cluster nodes for running Jepsen tests:
```bash
~/code/jepsen/yugabyte/setup-jepsen.sh
```

### Running tests

All commands described below should be run in `~/code/jepsen/yugabyte` directory.

In order to display help and see available tests and nemeses:
```bash
lein run test --help
```

To run test with specific nemesis, for example `start-stop-master`:
```bash
lein run test --nodes-file ~/code/jepsen/nodes --nemesis start-stop-master
```

To run all tests one by one under each nemesis in infinite loop:
```bash
./run-jepsen.py
```

This will also classify test results by categories and put them into `~/code/jepsen/yugabyte/results-sorted` 
sub-directories:
- *ok*
- *timed-out* - test run (including analysis phase) took more than time limit defined in `run-jepsen.py`.
- *no-history* - file with operations history is absent.
- *valid-unknown* - test results checker wasn't able to determine whether results are valid. 
- *invalid* - history of operations is inconsisent.

The following tests are implemented:

- `single-row-inserts` - concurrent row inserts into single table.
- `single-key-acid` - concurrent read, write, update if operations on a single row.
- `multi-key-acid` - concurrent writes to 2 different keys with the same value and reads.
- `counter-inc` - concurrent counter increments.
- `counter-inc-dec` - concurrent counter increments and decrements.
- `bank` - concurrent transfers between rows of a shared table.

Nemeses:

- `start-stop-tserver` - stops tserver process on a random node and then restarts.
- `start-kill-tserver` - kills tserver process on a random node and then restarts.
- `start-stop-master` - stops master process on a random node and then restarts.
- `start-kill-master` - kills master process on a random node and then restarts.
- `start-stop-node` - stops both tserver and master processes on a random node and then restarts.
- `start-kill-node` - kills both tserver and master processes on a random node and then restarts.
- `partition-random-halves` - cuts the network into randomly chosen halves.
- `partition-random-node` - isolates a single node from the rest of the network.
- `partition-majorities-ring` - random network partition where each node sees a majority of other nodes.
- `small-skew` - clock skews up to 100 ms.
- `medium-skew` - clock skews up to 250 ms.
- `large-skew` - clock skews up to 500 ms.
- `xlarge-skew` - clock skews up to 1000 ms.
