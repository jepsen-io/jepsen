# Adding a Set Test

We can model an etcd cluster as a set of registers, each identified by a key,
which support reads, writes, and compare-and-sets. But that's not the only
possible system we could build on top of etcd. We could, for instance, treat it
as a set of keys, and ignore the values altogether. Or we could implement a
queue on top of an etcd directory. In theory, we could model *every part* of
the etcd API, but the state space would be quite large, and the implementation
perhaps time-consuming. Typically, we'll focus on important or often-used parts
of the API.

But what makes a test *useful*? Our linearizable test is quite general,
performing different types of randomized operations, and determining whether
any pattern of those operations is linearizable. However, it is also quite
expensive. It'd be nice if we could design a test which is simple to verify,
but still tells us something useful.

Consider a set, supporting `add` and `read` operations. If we only read, our
test is trivially satisfied by seeing the empty set. If we only write, every
test will always pass, since it is always legal to add something to a set.
Clearly, we need a combination of reads and writes. Moreover, a read should be
the *last* thing that happens, since any writes *after* the final read couldn't
affect the outcome of the test.

What elements should we add? If we always add the same element, the test has
some resolving power: if every add returns `ok` but we don't read that element,
we know we've found a bug. However, if *any* add works, then the final read
will include that element, and we won't be able to tell if the other adds
actually worked or not. Perhaps it is most useful, then, to choose *distinct*
elements, so that every add operation has some independent effect on the read. If we choose ordered elements, we can get a rough picture of whether loss is evenly distributed over time, or occurs in chunks, so we'll do that as well.

Our operations, then, will be something like

```clj
{:type :invoke, :f :add, :value 0}
{:type :invoke, :f :add, :value 1}
...
{:type :invoke, :f :read, :value #{0 1}}
```

And we'll know the database performed correctly if every successful add is
present in the final read. We could obtain more information by performing
multiple reads, and tracking which have completed or are in-flight, but for
now, let's keep it simple.

## A New Namespace

It's starting to get a little cluttered in `jepsen.etcdemo`, so we're going to
break things up into a dedicated namespace for our new test. We'll call it `jepsen.etcdemo.set`:

```sh
$ mkdir src/jepsen/etcdemo
$ vim src/jepsen/etcdemo/set.clj
```

We'll be designing a new client and a generator, so we'll need those namespaces
from Jepsen. And of course we'll be using our etcd client library,
Verschlimmbesserung--and we'll want to handle exceptions from it, so that means
Slingshot too.

```clj
(ns jepsen.etcdemo.set
  (:require [jepsen
              [checker :as checker]
              [client :as client]
              [generator :as gen]]
            [slingshot.slingshot :refer [try+]]
            [verschlimmbesserung.core :as v]))
```

We'll need a new client that can add things to sets, and read them back--but we
have to choose how to store that set in the database. One option is to use
separate keys, or a pool of keys. Another is to use a single key, and have the
value be a serialized data type, like a JSON array or Clojure set. We'll do the latter.

```clj
(defrecord SetClient [k conn]
  client/Client
    (open! [this test node]
        (assoc this :conn (v/connect (client-url node)
```

Oh. That's a problem. We don't have `client-url` here. We could pull it in from
`jepsen.etcdemo`, but we'd like to require *this* namespace from
`jepsen.etcdemo` later, and Clojure tries very hard to avoid circular
dependencies between namespaces. Let's create a new, supporting namespace
called `jepsen.etcdemo.support`. Like `jepsen.etcdemo.set`, it'll have its own
file.

```bash
$ vim src/jepsen/etcdemo/support.clj
```

Let's move the url-constructing functions from `jepsen.etcdemo` to
`jepsen.etcdemo.support`.

```clj
(ns jepsen.etcdemo.support
  (:require [clojure.string :as str]))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" node ":" port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node 2380))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node 2379))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str node "=" (peer-url node))))
       (str/join ",")))
```

Now we'll require our support namespace from `jepsen.etcdemo`, and replace
calls to those functions with their new names:

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
					  ...
            [jepsen.etcdemo.support :as s]
            ...))

...

(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing etcd" version)
      (c/su
        (let [url (str "https://storage.googleapis.com/etcd/" version
                       "/etcd-" version "-linux-amd64.tar.gz")]
          (cu/install-archive! url dir))

        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :--log-output                   :stderr
          :--name                         node
          :--listen-peer-urls             (s/peer-url   node)
          :--listen-client-urls           (s/client-url node)
          :--advertise-client-urls        (s/client-url node)
          :--initial-cluster-state        :new
          :--initial-advertise-peer-urls  (s/peer-url node)
          :--initial-cluster              (s/initial-cluster test))

        (Thread/sleep 5000)))

...

    (assoc this :conn (v/connect (s/client-url node)
```

With that taken care of, back to `jepsen.etcdemo.set`. We'll require our
support namespace here too, and use it in the client.

```clj
(defrecord SetClient [k conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (s/client-url node)
                                 {:timeout 5000})))
```

We'll use the `setup!` function to initialize the value of a single key to an
empty Clojure set: `#{}`. We could hardcode the key again, but it'd be a bit
cleaner to have a field on the SetClient.

```clj
  (setup! [this test]
    (v/reset! conn k "#{}"))
```

Our `invoke!` function will look quite similar to our earlier client; we'll
dispatch based on `:f`, and use a similar error handler.

```clj
  (invoke! [_ test op]
    (try+
      (case (:f op)
        :read (assoc op
                     :type :ok
                     :value (read-string
                              (v/get conn k {:quorum? (:quorum test)})))
```

What about adding an element to the set? We need to read the current set, add
the new value, and write it back if it hasn't changed. Verschlimmbesserung has
a [helper for that](https://github.com/aphyr/verschlimmbesserung), `swap!`,
which takes a *function* to transform a key's value.

```clj
  (invoke! [_ test op]
    (try+
      (case (:f op)
        :read (assoc op
                     :type :ok,
                     :value (read-string
                              (v/get conn k {:quorum? (:quorum test)})))

        :add (do (v/swap! conn k (fn [value]
                                   (-> value
                                       read-string
                                       (conj (:value op))
                                       pr-str)))
                 (assoc op :type :ok)))

      (catch java.net.SocketTimeoutException e
        (assoc op
               :type  (if (= :read (:f op)) :fail :info)
               :error :timeout))))
```

We could clean up our key here, but for purposes of this tutorial, we'll skip
that part. It'll be deleted with all the rest of the data, when the test
starts up.

```clj
  (teardown! [_ test])

  (close! [_ test]))
```

Good! Now we need to package this up with a generator and a checker. We can use
the same name, OS, DB, and nemesis from the linearizable test, so instead of
making a *full* test map, we'll call this a "workload", and integrate it into
the test later.

Adding things to sets is such a common test that Jepsen has one built-in:
`checker/set`.


```clj
(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (SetClient. "a-set" nil)
   :checker   (checker/set)
   :generator
```

For the generator... hmm. Well, we know it proceeds in two parts: first, we're
going to add a bunch of things, and after that's done, we're going to perform a
single read. Let's write those two separately for now, and think about how to
combine them later.

How do we get a bunch of unique elements to add? We could write a generator
from scratch, but it might be easier to use Clojure's built-in sequence library
to construct a sequence of invoke ops, one for each number, and then wrap that
in a generator using `gen/seq`, like we did for the nemesis' infinite cycle of starts, sleeps, and stops.

```clj
(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client (SetClient. "a-set" nil)
   :checker (checker/set)
   :generator (->> (range)
                   (map (fn [x] {:type :invoke, :f :add, :value x})))
   :final-generator (gen/once {:type :invoke, :f :read, :value nil})})
```

For the final generator, we're using `gen/once` to emit a single read, instead
of an infinite sequence of reads.

## Integrating the New Workload

Now, we need to integrate this workload into the main `etcd-test`. Let's hop back to `jepsen.etcdemo`, and require the set test's namespace.

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
						...
            [jepsen.etcdemo [set :as set]
                            [support :as s]]
```

Looking at `etcd-test`, we could edit it directly, but we're going to want to
go *back* to our linearizable test eventually, so let's leave everything just
as it is, for now, and add a new map to override the client, checker, and
generator, based on the set workload.

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map. Special options:

      :quorum       Whether to use quorum reads
      :rate         Approximate number of requests per second, per thread
      :ops-per-key  Maximum number of operations allowed on any given key."
  [opts]
  (let [quorum    (boolean (:quorum opts))
        workload  (set/workload opts)]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name            (str "etcd q=" quorum)
            :quorum          quorum
            :os              debian/os
            :db              (db "v3.1.5")
            :client          (Client. nil)
            :nemesis         (nemesis/partition-random-halves)
            :checker         (checker/compose
                               {:perf   (checker/perf)
                                :indep (independent/checker
                                         (checker/compose
                                           {:linear   (checker/linearizable
                                                        {:model (model/cas-register)
                                                         :algorithm :linear})
                                            :timeline (timeline/html)}))})
            :generator       (->> (independent/concurrent-generator
                                    10
                                    (range)
                                    (fn [k]
                                      (->> (gen/mix [r w cas])
                                           (gen/stagger (/ (:rate opts)))
                                           (gen/limit (:ops-per-key opts)))))
                                  (gen/nemesis
                                    (->> [(gen/sleep 5)
                                          {:type :info, :f :start}
                                          (gen/sleep 5)
                                          {:type :info, :f :stop}]
                                         cycle))
                                  (gen/time-limit (:time-limit opts)))}
           {:client    (:client workload)
            :checker   (:checker workload)
```

Thinking about the generator a bit more... we know it's going to proceed in two
phases: adds, and a final read. We also know that we want the read to
*succeed*, which means we probably want the cluster to be nice and healed by
that point. So we'll perform normal nemesis operations in the `add` phase, then
stop the nemesis, wait a bit for the cluster to heal, and finally, perform our
read. `gen/phases` helps us write those kind of multi-stage generators.

```clj
            :generator (gen/phases
                         (->> (:generator workload)
                              (gen/stagger (/ (:rate opts)))
                              (gen/nemesis
                                (cycle [(gen/sleep 5)
                                        {:type :info, :f :start}
                                        (gen/sleep 5)
                                        {:type :info, :f :stop}]))
                              (gen/time-limit (:time-limit opts)))
                         (gen/log "Healing cluster")
                         (gen/nemesis (gen/once {:type :info, :f :stop}))
                         (gen/log "Waiting for recovery")
                         (gen/sleep 10)
                         (gen/clients (:final-generator workload)))})))
```

Let's give that a shot and see what happens.

```
$ lein run test --time-limit 10 --concurrency 10 -r 1/2
...
NFO [2018-02-04 22:13:53,085] jepsen worker 2 - jepsen.util 2	:invoke	:add	0
INFO [2018-02-04 22:13:53,116] jepsen worker 2 - jepsen.util 2	:ok	:add	0
INFO [2018-02-04 22:13:53,361] jepsen worker 2 - jepsen.util 2	:invoke	:add	1
INFO [2018-02-04 22:13:53,374] jepsen worker 2 - jepsen.util 2	:ok	:add	1
INFO [2018-02-04 22:13:53,377] jepsen worker 4 - jepsen.util 4	:invoke	:add	2
INFO [2018-02-04 22:13:53,396] jepsen worker 3 - jepsen.util 3	:invoke	:add	3
INFO [2018-02-04 22:13:53,396] jepsen worker 4 - jepsen.util 4	:ok	:add	2
INFO [2018-02-04 22:13:53,410] jepsen worker 3 - jepsen.util 3	:ok	:add	3
...
INFO [2018-02-04 22:14:06,934] jepsen nemesis - jepsen.generator Healing cluster
INFO [2018-02-04 22:14:06,936] jepsen nemesis - jepsen.util :nemesis	:info	:stop	nil
INFO [2018-02-04 22:14:07,142] jepsen nemesis - jepsen.util :nemesis	:info	:stop	:network-healed
INFO [2018-02-04 22:14:07,143] jepsen nemesis - jepsen.generator Waiting for recovery
...
INFO [2018-02-04 22:14:17,146] jepsen worker 4 - jepsen.util 4	:invoke	:read	nil
INFO [2018-02-04 22:14:17,153] jepsen worker 4 - jepsen.util 4	:ok	:read	#{0 7 20 27 1 24 55 39 46 4 54 15 48 50 21 31 32 40 33 13 22 36 41 43 29 44 6 28 51 25 34 17 3 12 2 23 47 35 19 11 9 5 14 45 53 26 16 38 30 10 18 52 42 37 8 49}
...
INFO [2018-02-04 22:14:29,553] main - jepsen.core {:valid? true,
 :lost "#{}",
 :recovered "#{}",
 :ok "#{0..55}",
 :recovered-frac 0,
 :unexpected-frac 0,
 :unexpected "#{}",
 :lost-frac 0,
 :ok-frac 1}


Everything looks good! ヽ(‘ー`)ノ
```

Look at that! 55 adds, all of which were preserved in the final read! If any
were lost, they'd show up in the `:lost` set.

Let's rewrite the linearizable register test as a workload, so it has the same shape as the set test.

```clj
(defn register-workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  {:client    (Client. nil)
   :checker   (independent/checker
                (checker/compose
                  {:linear   (checker/linearizable {:model     (model/cas-register)
                                                    :algorithm :linear})
                   :timeline (timeline/html)}))
```

We forgot about performance graphs! Those seem useful for *every* test, so
we'll leave them out of the workloads. For this particular workload, we need
independent checkers for both linearizability, and the HTML timelines. Next, we need the concurrent generator:

```clj
   :generator (independent/concurrent-generator
                10
                (range)
                (fn [k]
                  (->> (gen/mix [r w cas])
                       (gen/limit (:ops-per-key opts)))))})
```

This is a much simpler generator than before! The nemesis, rate limiting, and
time limits are applied for us by `etcd-test`, so we can leave them out of the
workload. We also don't need a :final-generator here, so we can leave that
blank--`nil` is a generator which means there's nothing left to do.

To switch between workloads, let's give each one a short name.

```clj
(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"set"      set/workload
   "register" register-workload})
```

Now, let's get rid of our register-specific code in `etcd-test`, and deal
purely with workloads. We'll take a string workload option, and use it to look
up the appropriate workload function, then call that with `opts` to build the
appropriate workload. We'll also modify our test name to include the workload
name.

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map. Special options:

      :quorum       Whether to use quorum reads
      :rate         Approximate number of requests per second, per thread
      :ops-per-key  Maximum number of operations allowed on any given key
      :workload     Type of workload."
  [opts]
  (let [quorum    (boolean (:quorum opts))
        workload  ((get workloads (:workload opts)) opts)]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name       (str "etcd q=" quorum " "
                             (name (:workload opts)))
            :quorum     quorum
            :os         debian/os
            :db         (db "v3.1.5")
            :nemesis    (nemesis/partition-random-halves)
            :client     (:client workload)
            :checker    (checker/compose
                          {:perf     (checker/perf)
                           :workload (:checker workload)})
   ...
```

Now, let's pass workload option specification to the CLI:

```clj
(def cli-opts
  "Additional command line options."
  [["-w" "--workload NAME" "What workload should we run?"
    :missing  (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   ...
```

We use `:missing` to have tools.cli insist on some value being provided. `cli/one-of` is a shortcut for ensuring that a value is a legal key in a map; it gives us helpful error messages. Now, if we run the test without a workload, it'll tell us that we need to choose a valid workload:

```bash
$ lein run test --time-limit 10 --concurrency 10 -r 1/2
--workload Must be one of register, set
```

And we can run either workload just by flipping the switch!

```bash
$ lein run test --time-limit 10 --concurrency 10 -r 1/2 -w set
...
$ lein run test --time-limit 10 --concurrency 10 -r 1/2 -w register
...
```

That's as far as we're going in this class! On your own, you might want to try
moving the register test into its own namespace, and splitting the set test to
use independent keys. Thanks for reading!
