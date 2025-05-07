# What's Here

This document provides an overview of Jepsen's various namespaces and how they
work together.

## Core Namespaces

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

Operations from the generator are passed to a
[jepsen.client](/jepsen/src/jepsen/client.clj), which applies them to the
database.

Operations are also passed to a
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

## Utilities

The OS, DBs, nemeses, and more use
[jepsen.control](/jepsen/src/jepsen/control.clj) supports running commands via
SSH (or other means) on DB nodes.
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
[jepsen.control.net](/jepsen/src/jepsen/control/net.clj), help with downloading URLs, making temporary files, getting IP addresses, and running daemons.
