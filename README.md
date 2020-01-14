# Jepsen

Breaking distributed systems so you don't have to.

Jepsen is a Clojure library. A test is a Clojure program which uses the Jepsen
library to set up a distributed system, run a bunch of operations against that
system, and verify that the history of those operations makes sense. Jepsen has
been used to verify everything from eventually-consistent commutative databases
to linearizable coordination systems to distributed task schedulers. It can
also generate graphs of performance and availability, helping you characterize
how a system responds to different faults. See
[jepsen.io](https://jepsen.io/analyses) for examples of the sorts of analyses
you can carry out with Jepsen.

[![Build Status](https://travis-ci.com/jepsen-io/jepsen.svg?branch=master)](https://travis-ci.com/jepsen-io/jepsen)

## Documentation

This [tutorial](doc/tutorial/index.md) walks you through writing a Jepsen test
from scratch.

For reference, see the [API documentation](http://jepsen-io.github.io/jepsen/).

## Design overview

A Jepsen test runs as a Clojure program on a *control node*. That program uses
SSH to log into a bunch of *db nodes*, where it sets up the distributed system
you're going to test using the test's pluggable *os* and *db*.

Once the system is running, the control node spins up a set of logically
single-threaded *processes*, each with its own *client* for the distributed
system. A *generator* generates new operations for each process to perform.
Processes then apply those operations to the system using their clients. The
start and end of each operation is recorded in a *history*. While performing
operations, a special *nemesis* process introduces faults into the system--also
scheduled by the generator.

Finally, the DB and OS are torn down. Jepsen uses a *checker* to analyze the
test's history for correctness, and to generate reports, graphs, etc. The test,
history, analysis, and any supplementary results are written to the filesystem
under `store/<test-name>/<date>/` for later review. Symlinks to the latest
results are maintained at each level for convenience.

## Setting up a Jepsen environment

Your control node needs a JVM and Leiningen 2 installed. Probably want JNA for
SSH auth too.

```sh
sudo apt-get install openjdk-8-jre openjdk-8-jre-headless libjna-java
```

For your db nodes, you'll need some (I use five) debian boxes. Most of these
tests are designed for Debian; either Jessie, for older versions of Jepsen, or
Stretch, for more recent versions. Some DBs don't need the latest packages so
you might get away with an older distribution, or possibly ubuntu. Each one
should be accessible from the control node via SSH. By default they're named
n1, n2, n3, n4, and n5, but that (along with SSH username, password, identity
files, etc) is all definable in your test. The account you use on those boxes
needs sudo access to set up DBs, control firewalls, etc.

Be advised that tests may mess with clocks, add apt repos, run killall -9 on
processes, and generally break things, so you shouldn't, you know, point jepsen
at your prod machines unless you like to live dangerously, or you wrote the
test and know exactly what it's doing.

You can run your DB nodes as separate physical machines, VMs, LXC instances, or
via Docker. Note that containers (LXC and Docker) can't change system clocks,
so you won't be able to test anything that relies on clock skew.

- You can launch a complete Jepsen cluster from the [AWS
  Marketplace](https://aws.amazon.com/marketplace/pp/B01LZ7Y7U0?qid=1486758124485&sr=0-1&ref_=srh_res_product_title).
  Choose "5+1 node cluster" to get an entire cluster as a Cloudformation stack,
  with SSH keys and firewall rules preconfigured. Choose "Single AMI" if you'd
  just like a single node.

- See [lxc.md](doc/lxc.md) for some of my notes on setting up LXC instances.

- You can also use [Docker Compose](docker/README.md) for setting up Docker instances.

## Running a test

Once you've got everything set up, you should be able to run `cd aerospike;
lein test`, and it'll spit out something like

```clj
INFO  jepsen.core - Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻

{:valid? false,
 :counter
 {:valid? false,
  :reads
  [[190 193 194]
   [199 200 201]
   [253 255 256]
   ...}}
```

## FAQ

### JSCH auth errors

If you see `com.jcraft.jsch.JSchException: Auth fail`, this means something
about your test's `:ssh` map is wrong, or your control node's SSH environment
is a bit weird.

0. Confirm that you can ssh to the node that Jepsen failed to connect to. Try
   `ssh -v` for verbose information--pay special attention to whether it uses a
   password or private key.
1. If you intend to use a username and password, confirm that they're specified
   correctly in your test's `:ssh` map.
2. If you intend to log in with a private key, make sure your SSH agent is
   running.
   - `ssh-add -l` should show the key you use to log in.
   - If your agent isn't running, try launching one with `ssh-agent`.
   - If your agent shows no keys, you might need to add it with `ssh-add`.
   - If you're SSHing to a control node, SSH might be forwarding your local
     agent's keys rather than using those on the control node. Try `ssh -a` to
     disable agent forwarding.

If you've SSHed to a DB node already, you might also encounter a jsch bug which
doesn't know how to read hashed known_hosts files. Remove all keys for the DB
hosts from your `known_hosts` file, then:

```sh
ssh-keyscan -t rsa n1 >> ~/.ssh/known_hosts
ssh-keyscan -t rsa n2 >> ~/.ssh/known_hosts
ssh-keyscan -t rsa n3 >> ~/.ssh/known_hosts
ssh-keyscan -t rsa n4 >> ~/.ssh/known_hosts
ssh-keyscan -t rsa n5 >> ~/.ssh/known_hosts
```

to add unhashed versions of each node's hostkey to your `~/.ssh/known_hosts`.
