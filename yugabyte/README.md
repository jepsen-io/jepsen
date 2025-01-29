# Jepsen tests for YugaByteDB

[YugaByteDB](https://github.com/YugaByte/yugabyte-db) is a transactional, high-performance database for building distributed cloud services developed by [YugaByte](http://www.yugabyte.com).

[Jepsen](https://github.com/aphyr/jepsen) is a testing framework for networked
databases, developed by Kyle 'Aphyr' Kingsbury to exercise and
[validate](https://jepsen.io) the claims to consistency made by database
developers or their documentation.

## What is being tested?

The tests run concurrent operations on different nodes in a YugaByteDB cluster
and checks that the operations preserve the consistency properties defined in
each test. During the tests, various combinations of nemeses can be added to
interfere with the database operations and exercise the database's consistency
protocols.

## Running

### Debian Jessie, with the Community Edition

Quickstart:

To run a single workload, use `lein run test`:

```
lein run test -o debian --version 1.2.10.0 --workload ycql/counter --nemesis partition
```

This command runs the set test against version 1.2.10.0, with network partitions, assuming nodes run Debian Jessie.

To run a full suite of tests, with various workloads and nemeses, use `lein run
test-all`

```
lein run test-all -o debian --version 1.2.10.0 --url https://downloads.yugabyte.com/yugabyte-ce-1.2.10.0-linux.tar.gz --concurrency 4n --time-limit 300 --only-workloads-expected-to-pass
```

Here, we're testing a specific pre-release tarball of version 1.1.15.0-b16.
We're running 4 clients per node, running for 300 seconds per test, and
constraining our run to only those workloads we think should pass.

#### Workloads

The following workloads are available with `--workload` (or `-w`).
Workloads have format `<api-name>/<test-name>`, where `<api-name>` is either `ycql` or `ysql`.

The following tests are available for both YCQL and YSQL:

- `counter` - concurrent counter increments.
- `set` - inserts single records and concurrently reads all of them back.
- `bank` - concurrent transfers between rows of a shared table.
- `long-fork` - looks for a snapshot isolation violation due to incompatible read orders.
- `single-key-acid` - each workers group is doing concurrent read, write, update-if operations on on their designated row.
- `multi-key-acid` - concurrent reads and write batches to a table with two-column composite key.

YCQL-specific tests:

- `set-index` - like set, but reads from a small pool of indices

YSQL-specific tests:

- `bank-multitable` - like bank, but across different tables.

#### Nemeses

The following nemeses are available with `--nemesis`. Nemeses can be combined
with commas, like `--nemesis partition,clock-skew`:

- `none` - no failures
- `clock-skew` - jumps and strobes in clocks, up to hundreds of seconds
- `partition`  - all kinds of network partitions
- `partition-half` - cuts the network into two halves, one with a majority
- `partition-one` - isolate a single node
- `partition-ring` - every node sees a majority, but no node sees the same set
- `kill` - kills and restarts tservers and masters
- `kill-tserver` - kill and restart tservers
- `kill-master` - kill and restart masters
- `stop` - stops and restarts tservers and masters
- `stop-tserver` - stops and restarts tservers
- `stop-master` - stops and restarts masters
- `pause` - pauses (with SIGSTOP) and resumes (with SIGCONT) tservers and masters
- `pause-tserver` - pauses tservers
- `pause-master` - pauses masters


### CentOS 7, Enterprise Edition

YugaByte's original version of these tests ran on CentOS 7, and used a
pre-installed Enterprise Edition cluster. We've preserved those codepaths in
this version of the tests (see `jepsen.auto`), but they haven't been tested,
and likely need some additional polish to work.

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

Install gnuplot:

```bash
sudo yum install gnuplot
```

#### YugaByteDB cluster setup

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

#### Wrapper scripts

These wrapper scripts were written for YugaByte's version of these tests, and
may no longer work correctly. They're preserved here in case anyone would like
to use them going forward. They aren't necessary to run the tests; the CLI interface for these tests can run all tests automatically.

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

