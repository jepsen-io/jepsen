# Database automation

In a Jepsen test, a `DB` encapsulates code for setting up and tearing down a
database, queue, or other distributed system we want to test. We could perform
the setup and teardown by hand, but letting Jepsen handle it lets us run tests
in a CI system, parameterize database configuration, run multiple tests
back-to-back with a clean slate, and so on.

In `src/jepsen/zookeeper.clj`, we'll require the `jepsen.db`, `jepsen.control`,
and `jepsen.os.debian` namespaces, aliasing each to a short name. We'll also
pull in every function from `clojure.tools.logging`, giving us log functions
like `info`, `warn`, etc.

```clj
(ns jepsen.zookeeper
  (:require [clojure.tools.logging :refer :all]
            [jepsen [db    :as db]
                    [control :as c]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]))
```

Then, we'll write a function that constructs a Jepsen DB, given a particular
version string.

```clj
(defn db
  "Zookeeper DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing ZK" version))

    (teardown! [_ test node]
      (info node "tearing down ZK"))))
```

The string after `(defn db ...` is a *docstring*, documenting the function's
behavior. When given a `version`, the `db` function uses `reify` to construct a
new object satisfying jepsen's `DB` protocol (from the `db` namespace). That
protocol specifies two functions all databases must support: `(setup db test
node)`, and `(teardown! db test node)`. We provide trivial implementations
here, which simply log an informational message.

Now, we'll extend the default `noop-test` by adding an `:os`, which tells
Jepsen how to handle operating system setup, and a `:db`, which we can
construct using the `db` function we just wrote:

```clj
(defn zk-test
  [version]
  (assoc tests/noop-test
         :os debian/os
         :db (db version)))
```

`noop-test`, like all Jepsen tests, is a map with keys like `:os`, `:name`,
`:db`, etc. See [jepsen.core](../../master/jepsen/src/jepsen/core.clj) for an overview of
test structure, and `jepsen.core/run` for the full definition of a test.

Right now `noop-test` has stub implementations for those keys. But we can
use `assoc` to build a copy of the `noop-test` map with *new* values for those
keys.

If we run this test, we'll see Jepsen using our code to set up debian,
tear down zookeeper, install ZK, then start its workers.

```bash
aphyr@waterhouse ~/j/jepsen.zookeeper (master)> lein test

lein test jepsen.zookeeper-test
INFO  jepsen.os.debian - :n1 setting up debian
INFO  jepsen.os.debian - :n5 setting up debian
INFO  jepsen.os.debian - :n2 setting up debian
INFO  jepsen.os.debian - :n3 setting up debian
INFO  jepsen.os.debian - :n4 setting up debian
INFO  jepsen.zookeeper - :n5 tearing down ZK
INFO  jepsen.zookeeper - :n1 tearing down ZK
INFO  jepsen.zookeeper - :n3 tearing down ZK
INFO  jepsen.zookeeper - :n2 tearing down ZK
INFO  jepsen.zookeeper - :n4 tearing down ZK
INFO  jepsen.zookeeper - :n2 installing ZK 3.4.5+dfsg-2
INFO  jepsen.zookeeper - :n3 installing ZK 3.4.5+dfsg-2
INFO  jepsen.zookeeper - :n1 installing ZK 3.4.5+dfsg-2
INFO  jepsen.zookeeper - :n5 installing ZK 3.4.5+dfsg-2
INFO  jepsen.zookeeper - :n4 installing ZK 3.4.5+dfsg-2
INFO  jepsen.core - nemesis done
INFO  jepsen.core - Worker 1 starting
...
INFO  jepsen.core - Everything looks good! ヽ(‘ー`)ノ

{:valid? true, :configs ({:model {}, :pending []}), :final-paths ()}


Ran 1 tests containing 1 assertions.
0 failures, 0 errors.
```

See how the version string `"3.4.5+dfsg-2"` was passed from
`zookeeper_test.clj` to `zk-test`, and in turn passed to `db`, where it was
*captured* by the `reify` object? This is how we *parameterize* Jepsen tests,
so the same code can test multiple versions or options. Note also that the
object `reify` returns closes over its lexical scope, *remembering* the value
of `version`.

## Installing the DB

With the skeleton of the DB in place, it's time to actually install something.
Jepsen's `debian` namespace has a function to ensure specific versions of
packages are installed, which we can use to install Zookeeper.

We'll have to be root to install packages, so we'll use `jepsen.control/su` to
assume root privileges. Note that `su` (and its companions `sudo`, `cd`, etc)
establish *dynamic*, not *lexical* scope--their effects apply not only to the
code they enclose, but down the call stack to any functions called.

```clj
(defn db
  "Zookeeper DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing ZK" version)
        (debian/install {:zookeeper version
                         :zookeeper-bin version
                         :zookeeperd version})))

    (teardown! [_ test node]
      (info node "tearing down ZK"))))
```

Running `lein test` will go and install zookeeper itself. Note that Jepsen runs
`setup` and `teardown` concurrently across all nodes.

## Configuration files

Next, we'll need to generate config files for each node. Each zookeeper node
needs an *id* number, so we need a way to generate a number for each node name.

```clj
(defn zk-node-ids
  "Returns a map of node names to node ids."
  [test]
  (->> test
       :nodes
       (map-indexed (fn [i node] [node i]))
       (into {})))
```

The `->>` threading macro takes each form and inserts it into the next form as
a final argument. So `(->> test :nodes)` becomes `(:nodes test)`, and `(->> test
:nodes (map-indexed (fn ...)))` becomes `(map-indexed (fn ...) (:nodes test))`,
and so on. Normal function calls often look "inside out", but the `->>` macro
lets us write a chain of operations "in order"--like an object-oriented
language's `foo.bar().baz()` notation.

So: in this function, we take the `test` map, extract the list of `:nodes` from
it, and map it, with numeric indices `0, 1, ...`, to `[node index]` pairs. Then
we take those pairs and insert them all into an empty map `{}`. The result: a
map of nodes to indices.

Given a map of nodes to IDs, computing the ID for any particular node is easy:

```clj
(defn zk-node-id
  "Given a test and a node name from that test, returns the ID for that node."
  [test node]
  ((zk-node-ids test) node))
```

We take advantage of the fact that maps are also functions: once we've
constructed the map of node names to IDs, we can just call that map with the
node name to get the corresponding ID.

Let's confirm that the node IDs we're generating look sane:

```clj
(defn db
  "Zookeeper DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing ZK" version)
        (debian/install {:zookeeper version
                         :zookeeper-bin version
                         :zookeeperd version})

        (info node "id is" (zk-node-id test node))))

    (teardown! [_ test node]
      (info node "tearing down ZK"))))
```

```bash
aphyr@waterhouse ~/j/jepsen.zookeeper (master)> lein test
...
INFO  jepsen.zookeeper - :n4 id is 3
INFO  jepsen.zookeeper - :n1 id is 0
INFO  jepsen.zookeeper - :n5 id is 4
INFO  jepsen.zookeeper - :n2 id is 1
INFO  jepsen.zookeeper - :n3 id is 2
```

Okay! Now let's write out that node ID to a file for each node:

```clj
(defn db
  "Zookeeper DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing ZK" version)
        (debian/install {:zookeeper version
                         :zookeeper-bin version
                         :zookeeperd version})

        (c/exec :echo (zk-node-id test node) :> "/etc/zookeeper/conf/myid")))

    (teardown! [_ test node]
      (info node "tearing down ZK"))))
```

We're using `jepsen.control/exec` to run a shell command. Jepsen automatically
binds `exec` to operate on the `node` being set up during `db/setup!`, but we
can connect to arbitrary nodes if we need to. Note that `exec` can take any
mixture of strings, numbers, keywords--it'll convert them to strings and
perform appropriate shell escaping. You can use `jepsen.control/lit` for an
unescaped literal string, if need be. `:>` and `:>>` perform shell redirection
as you'd expect.

```bash
$ lein test
...
$ ssh n1 cat /etc/zookeeper/conf/myid
0
```

And there's the node ID file! We also need a `zoo.cfg` configuration
file--we'll just adapt the default one from the debian package, and drop it in
`resources/zoo.cfg`:

```rb
# http://hadoop.apache.org/zookeeper/docs/current/zookeeperAdmin.html

# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial 
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between 
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
dataDir=/var/lib/zookeeper
# Place the dataLogDir to a separate physical disc for better performance
# dataLogDir=/disk2/zookeeper

# the port at which the clients will connect
clientPort=2181

# Leader accepts client connections. Default value is "yes". The leader machine
# coordinates updates. For higher update throughput at thes slight expense of
# read throughput the leader can be configured to not accept clients and focus
# on coordination.
leaderServes=yes
```

This file isn't ready yet--we also need to define the servers that will
participate in the cluster. We'll generate that section within the test. To do
that, we'll need a couple more namespaces: `clojure.string`, for string
manipulation, and `clojure.java.io`, for reading files from disk.

```clj
(ns jepsen.zookeeper
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            ...))
```

We'll compute the map of node names to node ids, then map each [k v] pair of
that map to a string like `"server.0=n1:2888:3888"`. Then we join those strings
with a newline.

```clj
(defn zoo-cfg-servers
  "Constructs a zoo.cfg fragment for servers."
  [test]
  (->> (zk-node-ids test)
       (map (fn [[node id]]
              (str "server." id "=" (name node) ":2888:3888")))
       (str/join "\n")))
```

Now we can combine the local `zoo.cfg` file with the dynamically generated
server fragment, and write that to the node's config file:

```clj
$ lein test
...
$ ssh n1 cat "/etc/zookeeper/conf/zoo.cfg"
# http://hadoop.apache.org/zookeeper/docs/current/zookeeperAdmin.html

# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial 
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between 
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
dataDir=/var/lib/zookeeper
# Place the dataLogDir to a separate physical disc for better performance
# dataLogDir=/disk2/zookeeper

# the port at which the clients will connect
clientPort=2181

# Leader accepts client connections. Default value is "yes". The leader machine
# coordinates updates. For higher update throughput at thes slight expense of
# read throughput the leader can be configured to not accept clients and focus
# on coordination.
leaderServes=yes

server.0=n1:2888:3888
server.1=n2:2888:3888
server.2=n3:2888:3888
server.3=n4:2888:3888
server.4=n5:2888:3888
```

Looks good! With the config files in place, we can add lifecycle management.

## Starting and stopping services

Debian ships with init scripts for Zookeeper, so we can use `service zookeeper`
to start and stop the daemon. After the daemon is stopped, we'll nuke the
node's data files and logs. Note that we use `jepsen.control/literal` to pass
through the *-glob unescaped.

```clj
(defn db
  "Zookeeper DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing ZK" version)
        (debian/install {:zookeeper version
                         :zookeeper-bin version
                         :zookeeperd version})

        (c/exec :echo (zk-node-id test node) :> "/etc/zookeeper/conf/myid")

        (c/exec :echo (str (slurp (io/resource "zoo.cfg"))
                           "\n"
                           (zoo-cfg-servers test))
                :> "/etc/zookeeper/conf/zoo.cfg")

        (info node "ZK restarting")
        (c/exec :service :zookeeper :restart)
        (info node "ZK ready")))

    (teardown! [_ test node]
      (info node "tearing down ZK")
      (c/su
        (c/exec :service :zookeeper :stop)
        (c/exec :rm :-rf
                (c/lit "/var/lib/zookeeper/version-*")
                (c/lit "/var/log/zookeeper/*"))))))
```

To make sure that every run starts fresh, even if a previous run crashed,
Jepsen performs a DB teardown at the start of the test, before setup. Then it
tears the DB down again at the conclusion of the test.

```bash
$ lein test
...
INFO  jepsen.zookeeper - :n5 tearing down ZK
INFO  jepsen.zookeeper - :n4 tearing down ZK
INFO  jepsen.zookeeper - :n1 tearing down ZK
INFO  jepsen.zookeeper - :n3 tearing down ZK
INFO  jepsen.zookeeper - :n2 tearing down ZK
INFO  jepsen.zookeeper - :n4 installing ZK 3.4.5+dfsg-2
INFO  jepsen.zookeeper - :n1 installing ZK 3.4.5+dfsg-2
INFO  jepsen.zookeeper - :n2 installing ZK 3.4.5+dfsg-2
INFO  jepsen.zookeeper - :n5 installing ZK 3.4.5+dfsg-2
INFO  jepsen.zookeeper - :n3 installing ZK 3.4.5+dfsg-2
INFO  jepsen.zookeeper - :n4 ZK restarting
INFO  jepsen.zookeeper - :n5 ZK restarting
INFO  jepsen.zookeeper - :n2 ZK restarting
INFO  jepsen.zookeeper - :n3 ZK restarting
INFO  jepsen.zookeeper - :n1 ZK restarting
INFO  jepsen.zookeeper - :n4 ZK ready
INFO  jepsen.zookeeper - :n2 ZK ready
INFO  jepsen.zookeeper - :n1 ZK ready
INFO  jepsen.zookeeper - :n3 ZK ready
INFO  jepsen.zookeeper - :n5 ZK ready
...
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.zookeeper/store/noop/20151231T185922.000-0800/history.txt
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.zookeeper/store/noop/20151231T185922.000-0800/results.edn
INFO  jepsen.zookeeper - :n1 tearing down ZK
INFO  jepsen.zookeeper - :n2 tearing down ZK
INFO  jepsen.zookeeper - :n5 tearing down ZK
INFO  jepsen.zookeeper - :n4 tearing down ZK
INFO  jepsen.zookeeper - :n3 tearing down ZK
INFO  jepsen.core - Everything looks good! ヽ(‘ー`)ノ
...
```

We can confirm that ZK shut down and the data files are empty:

```bash
$ ssh n1 ps aux | grep zoo
$ ssh n1 ls /var/lib/zookeeper/
myid
```

It'd be nice if we could see the DB logs for a given test as well. For this,
Jepsen provides an optional `db/LogFiles` protocol, specifying log paths to
download at the end of a test.

```clj
(defn db
  "Zookeeper DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node] ...)

    (teardown! [_ test node] ...)

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/zookeeper/zookeeper.log"])))
```

With the database ready, it's time to [write a client](client.md).
