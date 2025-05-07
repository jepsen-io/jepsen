# What's Here

This document provides an overview of Jepsen's various namespaces and how they
work together.

## Core Namespaces

These are the main namespaces involved in writing and running a test.

The main namespace is [jepsen.core](/jepsen/src/jepsen/core.clj). The core
function, `jepsen.core/run!` takes a test map, runs it, and returns a completed
test map.

[jepsen.cli](/jepsen/src/jepsen/cli.clj) supports command-line frontends to
Jepsen tests. It provides commands for running a single test, a family of
tests, and running a web server to browse results.

[jepsen.os](/jepsen/src/jepsen/os.clj) defines the protocol for an operating
system, which is used to install basic packages on a node and prepare it to run
a test. Most of the time you'll be using
[jepsen.os.debian](/jepsen/src/jepsen/os/debian.clj), which supports Debian
operating systems. There are also community-contributed, but generally
unsupported [jepsen.os.centos](/jepsen/src/jepsen/os/centos.clj),
[jepsen.os.smartos](/jepsen/src/jepsen/os/smartos.clj), and
[jepsen.os.ubuntu](/jepsen/src/jepsen/os/ubuntu.clj).

[jepsen.db](/jepsen/src/jepsen/db.clj) defines the core protocols for database
automation: installation, teardown, starting, killing, pausing, resuming,
downloading logs, and so on, along with some basic utilities.
[jepsen.db.watchdog](/jepsen/src/jepsen/db/watchdog.clj) supports restarting
databases.

[jepsen.generator](/jepsen/src/jepsen/generator.clj) generates the operations
Jepsen performs during a test. It provides (mostly) pure, composable data
structures which yield effects. During a test,
[jepsen.generator.interpreter](/jepsen/src/jepsen/generator/interpreter.clj) is
responsible for launching worker threads, drawing operations from the
generator, dispatching them, and journaling operations to the history. Two
internal libraries,
[jepsen.generator.translation-table](/jepsen/src/jepsen/generator/translation_table.clj)
and [jepsen.generator.context](/jepsen/src/jepsen/generator/context.clj), help
generators keep track of the time, what threads are doing, and
workload-specific context.
[jepsen.generator.test](/jepsen/src/jepsen/generator/test.clj) helps with
writing tests for generators.

Most operations from the generator are passed to a
[jepsen.client](/jepsen/src/jepsen/client.clj), which applies them to the
database. Clients also support basic lifecycle management: opening and closing
connections, and setting up and tearing down any state they need for the test.

Fault injection operations are passed to a
[jepsen.nemesis](/jepsen/src/jepsen/nemesis.clj), which injects faults; this
namespace provides some common faults and ways to compose nemeses together.
[jepsen.nemesis.combined](/jepsen/src/jepsen/nemesis/combined.clj) provides
composable packages of a nemesis, generators, and metadata.
[jepsen.nemesis.membership](/jepsen/src/jepsen/nemesis/membership.clj) and its
supporting protocol
[jepsen.nemesis.membership.state](/jepsen/src/jepsen/nemesis/membership/state.clj)
helps write nemeses that add and remove nodes from a cluster.
[jepsen.nemesis.time](/jepsen/src/jepsen/nemesis/time.clj) injects clock
errors. [jepsen.nemesis.file](/jepsen/src/jepsen/nemesis/file.clj) corrupts
files.

The OS, DBs, nemeses, and more use
[jepsen.control](/jepsen/src/jepsen/control.clj), which runs commands via SSH
(or other means) on DB nodes.
[jepsen.control.core](/jepsen/src/jepsen/control/core.clj) defines the basic
`Remote` protocol for running commands and transferring files, and some common
functions for shell escaping and error management. The implementations are in
[jepsen.control.clj-ssh](/jepsen/src/jepsen/control/clj_ssh.clj),
[jepsen.control.docker](/jepsen/src/jepsen/control/docker.clj),
[jepsen.control.k8s](/jepsen/src/jepsen.control/k8s.clj),
[jepsen.control.retry](/jepsen/src/jepsen/control/retry.clj),
[jepsen.control.scp](/jepsen/src/jepsen/control/scp.clj), and
[jepsen.control.sshj](/jepsen/src/jepsen/control/sshj.clj). Two utility
libraries, [jepsen.control.util](/jepsen/src/jepsen/control/util.clj) and
[jepsen.control.net](/jepsen/src/jepsen/control/net.clj), help with downloading
URLs, making temporary files, getting IP addresses, running daemons, etc.

The interpreter produces a
[jepsen.history](https://github.com/jepsen-io/history), which needs to be
analyzed. [jepsen.checker](/jepsen/src/jepsen/checker.clj) defines the protocol
for checking a history and composing checkers together. It also provides a slew
  of basic checkers for common problems, like computing overall statistics,
  finding unhandled errors in the database and test itself, checking sets and
  queues, and so on. [jepsen.checker.perf](/jepsen/src/jepsen/checker/perf.clj)
  renders performance plots.
  [jepsen.checker.timeline](/jepsen/src/jepsen/checker/timeline.clj) renders
  HTML timelines of a history.

[jepsen.store](/jepsen/src/jepsen/store.clj) writes tests to disk, as well as
auxiliary data files.
[jepsen.store.format](/jepsen/src/jepsen/store/format.clj) handles writing and
reading Jepsen's binary file format.
[jepsen.store.fressian](/jepsen/src/jepsen/store/fressian.clj) defines how
Jepsen serializes data with [Fressian](https://github.com/Datomic/fressian/).

## Utilities

These supporting libraries round out Jepsen's capabilities.

[jepsen.util](/jepsen/src/jepsen/util.clj) is the "kitchen sink" of Jepsen. It
provides a wealth of helpful utilities: everything from computing the majority
of `n` nodes to timeouts to random values

[jepsen.net](/jepsen/src/jepsen/net.clj), supported by
[jepsen.net.proto](/jepsen/src/jepsen/net/proto.clj), provides pluggable
support for network partitions between nodes, as well as losing or slowing down
packets.

[jepsen.reconnect](/jepsen/src/jepsen/reconnect.clj) provides automatic
reconnection and retries for flaky clients. It's used internally by
`jepsen.control`'s SSH remotes, and also DB clients.

[jepsen.fs-cache](/jepsen/src/jepsen/fs-cache.clj) provides a local (e.g. on
the control node) cache for arbitrary data structures and files. This is
helpful when a database requires an expensive one-time setup step.

[jepsen.codec](/jepsen/src/jepsen/codec.clj) encodes and decodes values to strings, for testing systems which store everything in string blobs.

[jepsen.web](/jepsen/src/jepsen/web.clj) is Jepsen's web interface, for browsing the `store/` directory.

[jepsen.repl](/jepsen/src/jepsen/repl.clj) is where `lein repl` drops you.

## Tests

These namespaces are oriented towards running a specific kind of test, like a
list-append test. They usually provide a package of a generator, client, and
checker together.

[jepsen.tests](/jepsen/src/jepsen/tests.clj) provides a no-op test map, as a
stub for writing new tests.

[jepsen.tests.adya](/jepsen/src/jepsen/tests/adya.clj) provides a test for a
specific kind of G2 anomaly. It's replaced by `jepsen.tests.cycle`.

[jepsen.tests.bank](/jepsen/src/jepsen/tests/bank.clj) models transfers between
a pool of bank accounts, and checks to make sure the total balance is
conserved.

[jepsen.tests.causal-reverse](/jepsen/src/jepsen/tests/causal_reverse.clj)
looks for a violation of Strict Serializability where T1 < T2, but T2 is
visible without T1. It's replaced by `jepsen.tests.cycle`.

[jepsen.tests.cycle](/jepsen/src/jepsen/tests/cycle.clj) contains two workloads, [jepsen.tests.cycle.append](/jepsen/src/jepsen/tests/cycle/append.clj) and [jepsen.tests.cycle.wr](/jepsen/src/jepsen/tests/cycle/wr.clj), which are small wrappers around [Elle](https://github.com/jepsen-io/elle), a sophisticated transactional safety checker.

[jepsen.tests.kafka](/jepsen/src/jepsen/tests/kafka.clj) is for testing
Kafka-style logs, in which clients publish messages to totally-ordered logs,
and subscribe to messages from them.

[jepsen.tests.linearizable-register](/jepsen/src/jepsen/tests/linearizable_register.clj)
is for testing a family of independent Linearizable registers. Useful for
systems like etcd or Consul.

[jepsen.tests.long-fork](/jepsen/src/jepsen/tests/long_fork.clj) finds Long
Fork anomalies. It's mostly superseded by jepsen.tests.cycle.list-append.

## Extensions

These namespaces extend Jepsen to new workloads, faults, or styles of testing.

[jepsen.independent](/jepsen/src/jepsen/independent.clj) lets you take a
generator, client, and checker, and run several independent copies of them in
the same test, either sequentially or concurrently. This is helpful when a
workload needs to be "small"--say only a few keys or a few operations--but
you'd like to do lots of copies of it through the course of a test.

[jepsen.role](/jepsen/src/jepsen/role.clj) supports tests where nodes have
distinct roles. For instance, some might handle storage, and others
transactions. It provides composable `DB`, `Client`, `Nemesis`, and `Generator`
code to glue together these different subsystems.

[jepsen.faketime](/jepsen/src/jepsen/faketime.clj) helps run databases within
an `LD_PRELOAD` shim that lies about the system clock.

[jepsen.lazyfs](/jepsen/src/jepsen/lazyfs.clj) runs databases within a
directory that can lose un-fsynced writes on command.

