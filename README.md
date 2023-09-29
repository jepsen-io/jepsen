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

[![Clojars Project](https://img.shields.io/clojars/v/jepsen.svg)](https://clojars.org/jepsen)
[![Build Status](https://travis-ci.com/jepsen-io/jepsen.svg?branch=main)](https://travis-ci.com/jepsen-io/jepsen)

## Design Overview

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

## Documentation

This [tutorial](doc/tutorial/index.md) walks you through writing a Jepsen test
from scratch.

For reference, see the [API documentation](http://jepsen-io.github.io/jepsen/).

An independent translation is available in [Chinese](https://jaydenwen123.gitbook.io/zh_jepsen_doc/).

## Setting up a Jepsen Environment

So, you've got a Jepsen test, and you'd like to run it! Or maybe you'd like to
start learning how to write tests. You've got several options:

### AWS

If you have an AWS account, you can launch a full Jepsen cluster---control and
DB nodes---from the [AWS
Marketplace](https://aws.amazon.com/marketplace/pp/Jepsen-LLC-Jepsen/B01LZ7Y7U0).
Click "Continue to Subscribe", "Continue to Configuration", and choose
"CloudFormation Template". You can choose the number of nodes you'd like to
deploy, adjust the instance types and disk sizes, and so on. These are full
VMs, which means they can test clock skew.

The AWS marketplace clusters come with an hourly fee (generally $1/hr/node),
which helps fund Jepsen development.

### LXC

You can set up your DB nodes as LXC containers, and use your local machine as
the control node. See the [LXC documentation](doc/lxc.md) for guidelines. This
might be the easiest setup for hacking on tests: you'll be able to edit source
code, run profilers, etc on the local node. Containers don't have real clocks,
so you generally can't use them to test clock skew.

### VMs, Real Hardware, etc.

You should be able to run Jepsen against almost any machines which have:

- A TCP network
- An SSH server
- Sudo or root access

Each DB node should be accessible from the control node via SSH: you need to be
able to run `ssh myuser@some-node`, and get a shell. By default, DB nodes are
named n1, n2, n3, n4, and n5, but that (along with SSH username, password,
identity files, etc) is all definable in your test, or at the CLI. The account
you use on those boxes needs sudo access to set up DBs, control firewalls, etc.

BE ADVISED: tests may mess with clocks, add apt repos, run killall -9 on
processes, and generally break things, so you shouldn't, you know, point Jepsen
at your prod machines unless you like to live dangerously, or you wrote the
test and know exactly what it's doing.

NOTE: Most Jepsen tests are written with more specific requirements in
mind---like running on Debian, using `iptables` for network manipulation, etc.
See the specific test code for more details.

### Docker (Unsupported)

There is a [Docker Compose setup](/docker) for running a Jepsen cluster on a
single machine. Sadly the Docker platform has been something of a moving
target; this environment tends to break in new and exciting ways on various
platforms every few months. If you're a Docker whiz and can get this going
reliably on Debian & OS X that's great--pull requests would be a big help.

Like other containers Docker containers don't have real clocks--that means you
generally can't use them to test clock skew.

### Setting Up Control Nodes

For AWS and Docker installs, your control node comes preconfigured with all the
software you'll need to run most Jepsen tests. If you build your own control
node (or if you're using your local machine as a control node), you'll need a
few things:

- A [JVM](https://openjdk.java.net/install/)---version 1.8 or higher.
- JNA, so the JVM can talk to your SSH.
- [Leiningen](https://leiningen.org/): a Clojure build tool.
- [Gnuplot](http://www.gnuplot.info/): how Jepsen renders performance plots.
- [Graphviz](https://graphviz.org/): how Jepsen renders transactional anomalies.

On Debian, try:

```
sudo apt install openjdk-17-jdk libjna-java gnuplot graphviz
```

... to get the basic requirements in place. Debian's Leiningen packages are
ancient, so [download lein from the web instead](https://leiningen.org/).

## Running a Test

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

## Working With the REPL

Jepsen tests emit `.jepsen` files in the `store/` directory. You can use these
to investigate a test at the repl. Run `lein repl` in the test directory (which
should contain `store...`, then load a test using `store/test`:

```clj
user=> (def t (store/test -1))
```

-1 is the last test run, -2 is the second-to-last. 0 is the first, 1 is the
second, and so on. You can also load a by the string directory name. As a handy
shortcut, clicking on the title of a test in the web interface will copy its
path to the clipboard.

```clj
user=> (def t (store/test "/home/aphyr/jepsen.etcd/store/etcd append etcdctl kill/20221003T124714.485-0400"))
```

These have the same structure as the test maps you're used to working with in
Jepsen, though without some fields that wouldn't make sense to serialize--no
`:checker`, `:client`, etc.

```clj
jepsen.etcd=> (:name t)
"etcd append etcdctl kill"
jepsen.etcd=> (:ops-per-key t)
200
```

These test maps are also lazy: to speed up working at the REPL, they won't load
the history or results until you ask for them. Then they're loaded from disk
and cached.

```clj
jepsen.etcd=> (count (:history t))
52634
```

You can use all the usual Clojure tricks to introspect results and histories.
Here's an aborted read (G1a) anomaly--we'll pull out the ops which wrote and
read the aborted read:

```clj
jepsen.etcd=> (def writer (-> t :results :workload :anomalies :G1a first :writer))
#'jepsen.etcd/writer
jepsen.etcd=> (def reader (-> t :results :workload :anomalies :G1a first :op))
#'jepsen.etcd/reader
```

The writer appended 11 and 12 to key 559, but failed, returning a duplicate key
error:

```clj
jepsen.etcd=> (:value writer)
[[:r 559 nil] [:r 558 nil] [:append 559 11] [:append 559 12]]
jepsen.etcd=> (:error writer)
[:duplicate-key "rpc error: code = InvalidArgument desc = etcdserver: duplicate key given in txn request"]
```

The reader, however, observed a value for 559 beginning with 12!

```clj
jepsen.etcd=> (:value reader)
[[:r 559 [12]] [:r 557 [1]]]
```

Let's find all successful transactions:

```clj
jepsen.etcd=> (def txns (->> t :history (filter #(and (= :txn (:f %)) (= :ok (:type %)))) (map :value)))
#'jepsen.etcd/txns
```

And restrict those to just operations which affected key 559:

```clj
jepsen.etcd=> (->> txns (filter (partial some (comp #{559} second))) pprint)
([[:r 559 [12]] [:r 557 [1]]]
 [[:r 559 [12]] [:append 559 1] [:r 559 [12 1]]]
 [[:append 556 32]
  [:r 556 [1 18 29 32]]
  [:r 556 [1 18 29 32]]
  [:r 559 [12 1]]]
 [[:r 559 [12 1]]]
 [[:append 559 9] [:r 557 [1 5]] [:r 558 [1]] [:r 558 [1]]]
 [[:r 559 [12 1 9]] [:r 559 [12 1 9]]]
 [[:append 559 17]]
 [[:r 559 [12 1 9 17]] [:append 558 5]]
 [[:r 559 [12 1 9 17]]
  [:append 557 22]
  [:append 559 27]
  [:r 557 [1 5 12 22]]])
```

Sure enough, no OK appends of 12 to key 559!

You'll find more functions for slicing-and-dicing tests in `jepsen.store`.

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

### SSHJ auth errors

If you get an exception like `net.schmizz.sshj.transport.TransportException:
Could not verify 'ssh-ed25519' host key with fingerprint 'bf:4a:...' for 'n1'
on port 22`, but you're sure you've got the keys in your `~/.ssh/known-hosts`,
this is because (I think) SSHJ tries to verify only the ed25519 key and
*ignores* the RSA key. You can add the ed25519 keys explicitly via:

```sh
ssh-keyscan -t ed25519 n1 >> ~/.ssh/known_hosts
...
```

## Other Projects

Additional projects that may be of interest:

- [Jecci](https://github.com/michaelzenz/jecci): A wrapper framework around Jepsen
- [Porcupine](https://github.com/anishathalye/porcupine): a linearizability checker written in Go.
