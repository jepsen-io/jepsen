# Jepsen

Breaking distributed systems so you don't have to.

Jepsen is a Clojure library. A test is a Clojure program which uses the Jepsen
library to set up a distributed system, run a bunch of operations against that
system, and verify that the history of those operations makes sense. Jepsen has
been used to verify everything from eventually-consistent commutative databases
to linearizable coordination systems to distributed task schedulers. It can
also generate graphs of performance and availability, helping you characterize
how a system responds to different faults. See
[aphyr.com](https://aphyr.com/tags/jepsen) for examples of the sorts of
analyses you can carry out with Jepsen.

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

Your local machine needs a JVM and leiningen 2 installed. Probably want JNA for SSH auth too.

```sh
sudo apt-get install openjdk-8-jre openjdk-8-jre-headless libjna-java
```

For your db nodes, you'll need some (I use five) debian boxes. I run debian
jessie, but some DBs don't need the latest packages so you might get away with
an older distribution, or possibly ubuntu. Each one should be accessible from
the control node via SSH. By default they're named n1, n2, n3, n4, and n5, but
that (along with SSH username, password, identity files, etc) is all definable
in your test. The account you use on those boxes needs sudo access to set up
DBs, control firewalls, etc.

Be advised that tests may mess with clocks, add apt repos, run killall -9 on
processes, and generally break things, so you shouldn't, you know, point jepsen
at your prod machines unless you like to live dangerously, or you wrote the
test and know exactly what it's doing.

See [lxc.md](doc/lxc.md) for some of my notes on setting up LXC instances.

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

## Writing a test

If you don't know Clojure, you'll want to learn some of the basics. Try
[Clojure From the Ground
Up](https://aphyr.com/posts/301-clojure-from-the-ground-up-welcome) and
[Clojure for the Brave and True](http://www.braveclojure.com/). Or you can
reimplement Jepsen's ideas in a language you *do* know. Either way's fine!

Jepsen tests are (surprise!) data structures. This makes it easier to
programatically compose tests out of reusable, parameterizable pieces. See
[jepsen.core](jepsen/src/jepsen/core.clj) for an overview of test structure,
and `jepsen.core/run` for the full definition of a test.

### Scaffolding

To start a new test for a database called "meowdb", create a new project using
leiningen:

```sh
lein new jepsen.meowdb
```

In the `jepsen.meowdb` directory you'll find a `project.clj`, which defines the project's dependencies. Add the [latest version](https://clojars.org/jepsen) of Jepsen, and whatever JVM libraries you'll need to talk to your database.

```clj
(defproject jepsen.meowdb "0.1.0-SNAPSHOT"
  :description "A Jepsen test for meowdb"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [jepsen "0.0.6"]
                 [com.meowdb/meowdb-client "1.2.3"]])
```

We'll build up a Jepsen test in `src/jepsen/meowdb.clj` and run it from
`test/jepsen/meowdb_test.clj`. Our test namespace is gonna use
`jepsen.core/run!` to run a test--something like this:

```clj
(ns jepsen.meowdb-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.meowdb :as meowdb]))

(def version
  "What meowdb version should we test?"
  "1.2.3")

(deftest basic-test
  (is (:valid? (:results (run! (meowdb/basic-test version))))))
```

Now we have to *write* that test for meowdb. This part's a little tougher. In
`src/jepsen/meowdb.clj` we'll suck in a bunch of useful namespaces from Jepsen,

```clj
(ns jepsen.meowdb
  "Tests for MeowDB"
  (:require [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [knossos.op :as op]
            [jepsen [client :as client]
                    [core :as jepsen]
                    [db :as db]
                    [tests :as tests]
                    [control :as c :refer [|]]
                    [checker :as checker]
                    [nemesis :as nemesis]
                    [generator :as gen]
                    [util :refer [timeout meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]))
```

And define a function that takes a version and spits out a Jepsen test for it:

```clj
(defn basic-test
  "A simple test of MeowDB's safety."
  [version]
  tests/noop-test)
```

That's the function we called from `jepsen.meowdb-test`. Let's run the test
now:

```
$ lein test

Testing jepsen.meowdb-test
INFO  jepsen.core - nemesis done
INFO  jepsen.core - Worker 0 starting
INFO  jepsen.core - Worker 4 starting
INFO  jepsen.core - Worker 0 done
INFO  jepsen.core - Worker 4 done
INFO  jepsen.core - Worker 1 starting
INFO  jepsen.core - Worker 2 starting
INFO  jepsen.core - Worker 3 starting
INFO  jepsen.core - Worker 2 done
INFO  jepsen.core - Worker 1 done
INFO  jepsen.core - Worker 3 done
INFO  jepsen.core - Run complete, writing
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.meowdb/store/noop/20150925T133126.000-0700/history.txt
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.meowdb/store/noop/20150925T133126.000-0700/results.edn
INFO  jepsen.core - Analyzing
INFO  jepsen.core - Analysis complete
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.meowdb/store/noop/20150925T133126.000-0700/history.txt
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.meowdb/store/noop/20150925T133126.000-0700/results.edn
INFO  jepsen.core - Everything looks good! ヽ(‘ー`)ノ

{:valid? true,
 :linearizable-prefix [],
 :worlds ({:model {}, :fixed [], :pending #{}, :index 0})}


Ran 1 tests containing 1 assertions.
0 failures, 0 errors.
```

OK, that's a start.

### Writing a DB

`jepsen.db` defines a protocol for database lifecycle management--setting up
and tearing down a DB between tests. We'll add a function that constructs a DB
which closes over a particular version.

```clj
(defn db
  "Sets up and tears down MeowDB"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "set up"))

    (teardown! [_ test node]
      (info node "tore down"))))

(defn basic-test
  "A simple test of MeowDB's safety."
  [version]
  (merge tests/noop-test
         {:os debian/os
          :db (db version)}))
```

If you re-run the tests, you'll see Jepsen tearing down, starting up, and
finally tearing down the database during the test. Well, it's not actually
*doing* anything yet, but the log lines are there. :)

Jepsen automatically invokes setup and teardown on each node concurrently.
`jepsen.control` provides functions for shell operations, and you'll find
supplementary package-management things in `jepsen.os.debian`. For example:

```clj
; All calls within the su macro run as root
(c/su

  ; Pull a file from resources/apt-prefs and write it to the remote filesystem
  (c/exec :echo (slurp (io/resource "apt-prefs"))
          :> "/etc/apt/preferences.d/00percona.pref")

  ; Install specific versions of packages
  (debian/install {:percona-xtradb-cluster-56 version})

  ; Arbitrary shell commands
  (c/exec :service :mysql :stop))
```

Since this is all code, you can break up the setup and teardown functions into
smaller functions. Config files and the like can be stored in `resources/`, and
you can template them for deployment for various nodes. See [the Percona
test](percona/src/jepsen/percona.clj) for examples.

### Writing a generator

Jepsen is focused around *operations*, which are Clojure maps with the
following mandatory keys:

```clj
{:process   Which process evaluated this operation?
 :type      One of:
              - :invoke    This operation is beginning
              - :ok        The operation completed successfully
              - :fail      We know the operation didn't happen
              - :info      The operation crashed; it may take place at any
                           future time. The process which invoked it will never
                           be re-used; jepsen.core will spawn a new process and
                           client to maintain constant concurrency.
 :f         The function we're evaluating--e.g. :read, :write, :append, :lock
 :value     The value which was read/written/appended etc.}
```

`:f` and `:value` are completely arbitrary; you choose what functions exist in
your domain model. The only thing that matters is that the client and checker
agree on operation semantics.

A generator is a single stateful object that yields :invoke operations to
processes. Each process asks the generator for an operation, applies it to the
client, then comes back for another operation. Once a generator returns nil,
it's empty and a process won't request any more from it. See
[jepsen.generator](jepsen/src/jepsen/generator.clj) for more details.

Complex test schedules are built by composing simpler generators. Functions can
be generators, as can literal operations (they return themselves). You can
lift a sequence of ops into a generator, or write one completely from scratch.

```clj
(defn bank-read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [test process]
  (let [n (-> test :client :n)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-int n)
             :to     (rand-int n)
             :amount (rand-int 5)}}))

(defn generator []
  (gen/phases
    (->> (gen/mix [bank-read bank-diff-transfer])
         (gen/clients)
         (gen/stagger 1/10)
         (gen/time-limit 100))
    (gen/log "waiting for quiescence")
    (gen/sleep 10)
    (gen/clients (gen/once bank-read)))
```

Note that generators need not provide a process ID; it will be automatically
assigned by Jepsen.

Bind your generator into your test like so:

```clj
(merge noop-test
       {...
        :generator (generator)})
```

Remember, generators are *stateful* and cannot be re-used across tests. We have
to construct a fresh one for every test.

### Writing a client

Clients take `:invoke` operations, apply them to the system being tested (e.g.
by making a network call) and return a corresponding completion operation with
type `:ok`, `:fail`, or `:info`).

Clients have a stateful lifecycle, so you can bind resources like network
sockets and release them when complete. The client you pass to your test is
like a pluripotent stem cell--it is never used for applying operations, but can
be *specialized* for use by a particular process. When a process starts, it
calls `(client/setup! original-client test some-node)`, which returns a copy of
the client with any necessary resources allocated.

When Jepsen is done with a client, it calls `(client/teardown! client test)`,
and you can release resources there. For example, here's the client for an
Aerospike test which implements a compare-and-set register. See how `setup`
returns a *copy* of the client (`this`) with a fresh Aerospike client attached.

```clj
(defrecord CasRegisterClient [client namespace set key]
  client/Client
  (setup! [this test node]
    (let [client (connect node)]
      (put! client namespace set key {:value nil})
      (assoc this :client client)))

  (invoke! [this test op]
    (with-errors op #{:read}
      (case (:f op)
        :read (assoc op
                     :type :ok,
                     :value (-> client (fetch namespace set key) :bins :value))

        :cas   (let [[v v'] (:value op)]
                 (cas! client namespace set key
                       (fn [r]
                         ; Verify that the current value is what we're cas'ing
                         ; from
                         (when (not= v (:value r))
                           (throw (ex-info "skipping cas" {})))
                         {:value v'}))
                 (assoc op :type :ok))

        :write (do (put! client namespace set key {:value (:value op)})
                   (assoc op :type :ok)))))

  (teardown! [this test]
    (close client)))

(defn cas-register-client
  "A basic CAS register on top of a single key and bin."
  []
  (CasRegisterClient. nil "jepsen" "cats" "mew"))
```

Note that `invoke!` always returns a copy of the invocation operation but with
`:type :ok`. The `with-errors` macro catches exceptions and returns `:type
:info` instead.

A wrapper function, `cas-register-client`, provides some default arguments for
setting up a fresh client. Add a client to your test by merging in `:client
(cas-register-client)`.

### Writing a checker

See [jepsen/src/jepsen/checker.clj](jepsen.checker) for some example checkers;
Jepsen ships with checkers for counters, eventually consistent sets, queues,
and linearizable systems, as well as some visualizations--for instance, latency
and throughput graphs, both as scatterplots and quantiles. You can also write
your own. For instance, this one from the Percona tests verifies every
successful read in the history sees the correct total balance across all
accounts, and that no balance is negative.

```clj
(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test model history]
      (let [bad-reads (->> history
                           (r/filter op/ok?)
                           (r/filter #(= :read (:f %)))
                           (r/map (fn [op]
                                  (let [balances (:value op)]
                                    (cond (not= (:n model) (count balances))
                                          {:type :wrong-n
                                           :expected (:n model)
                                           :found    (count balances)
                                           :op       op}

                                         (not= (:total model)
                                               (reduce + balances))
                                         {:type :wrong-total
                                          :expected (:total model)
                                          :found    (reduce + balances)
                                          :op       op}))))
                           (r/filter identity)
                           (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads})))
```

This checker needs some information about the expected behavior of the system
from the test's `:model`--in this case, the model is just a map with a `:n` key
with the expected number of balances, and a `:total` key for the expected sum.
The linearizability checker uses the model to verify singlethreaded execution
over a datatype. Other checkers don't use a model at all.

Checkers always return a `:valid?` key which is true if the test passed. You
can compose multiple checkers together; the composed checker is valid if all
subcheckers pass.

```clj
(merge noop-test
  ...
  :checker (checker/compose {:perf (checker/perf)
                             :bank (bank-checker)}))
```

## FAQ

### JSCH auth errors

You might be hitting a jsch bug which doesn't know how to read hashed
known_hosts files. Remove all keys for the DB hosts from your `known_hosts`
file, then:

```sh
ssh-keyscan -t rsa n1 >> ~/.ssh/known_hosts
ssh-keyscan -t rsa n2 >> ~/.ssh/known_hosts
ssh-keyscan -t rsa n3 >> ~/.ssh/known_hosts
ssh-keyscan -t rsa n4 >> ~/.ssh/known_hosts
ssh-keyscan -t rsa n5 >> ~/.ssh/known_hosts
```

to add unhashed versions of each node's hostkey to your `~/.ssh/known_hosts`.
