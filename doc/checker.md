# Adding a checker

With our generator and clients performing operations, we've got a history to
analyze for correctness. Jepsen uses a *model* to represent the abstract
behavior of a system, and a *checker* to verify whether the history conforms to
that model. We'll require `knossos.model` and `jepsen.checker`:

```clj
(ns jepsen.etcdemo
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [slingshot.slingshot :refer [try+]]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [tests :as tests]
                    [util :as util :refer [timeout]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
```

Remember how we chose to model our operations as reads, writes, and cas operations?

```clj
(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})
```

Jepsen doesn't know what `:f :read` or `:f :cas` mean. As far as it's
concerned, they're arbitrary values. However, our *client* understands how to
interpret those operations, when it dispatches based on `(case (:f op) :read
...)`. Now we need a *model* of the system which understands those same
operations. Knossos defines a Model data type for us, which takes a model and an
operation to apply, and returns a new model resulting from that operation. Here's what that looks like:

```clj
(definterface+ Model
  (step [model op]
        "The job of a model is to *validate* that a sequence of operations
        applied to it is consistent. Each invocation of (step model op)
        returns a new state of the model, or, if the operation was
        inconsistent with the model's state, returns a (knossos/inconsistent
        msg). (reduce step model history) then validates that a particular
        history is valid, and returns the final state of the model.
        Models should be a pure, deterministic function of their state and an
        operation's :f and :value."))
```

It turns out that the Knossos checker defines some common models for things
like locks and registers. Here's one from
[knossos.model](https://github.com/jepsen-io/knossos/blob/443a5a081c76be315eb01c7990cc7f1d9e41ed9b/src/knossos/model.clj#L66-L80).

```clj
(defrecord CASRegister [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (CASRegister. (:value op))
      :cas   (let [[cur new] (:value op)]
               (if (= cur value)
                 (CASRegister. new)
                 (inconsistent (str "can't CAS " value " from " cur
                                    " to " new))))
      :read  (if (or (nil? (:value op))
                     (= value (:value op)))
               r
               (inconsistent (str "can't read " (:value op)
                                  " from register " value))))))
```
We don't need to write these in our tests. I just want you to know how they
work.

This defrecord defines a new data type called `CASRegister`, which has a
single, immutable field, called `value`. It satisfies the `Model` interface we
discussed earlier, and its `step` function takes a current register `r`, and an
operation `op`. When we want to write a new value, we simply return a new
`CASRegister` with that value assigned. To compare-and-set from one value to
another, we pull apart the current and new values from the operation, and if
the current and new values match, construct a fresh register with the new
value. If they don't match, we return a special type of model with
`inconsistent`, which indicates that that operation could not be applied to the
register. Reads are similar, except that we always allow reads of `nil` to pass
through. This allows us to satisfy histories which include reads that never
returned.

We'll add a model to our test by constructing a fresh `CASRegister` with a
`nil` value--to match the `nil` value of an uninitialized node in etcd.

```clj
(defn etcd-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :client (client nil)
          :model  (model/cas-register)
          :checker checker/linearizable
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/clients)
                          (gen/time-limit 15))}
         opts))
```

To analyze the history, we'll specify a `:checker` for the test.
`checker/linearizable` uses the Knossos linearizability checker to verify that
every operation appears take place atomically between its invocation and
completion. It'll use the test's `:model` to specify how the system *should*
behave.

Running the test, we can confirm the checker's results:

```bash
$ lein run test
...
INFO [2017-03-31 17:38:16,855] jepsen worker 0 - jepsen.util 0  :invoke :write  1
INFO [2017-03-31 17:38:16,861] jepsen worker 0 - jepsen.util 0  :ok :write  1
...
INFO [2017-03-31 17:38:19,307] main - jepsen.core {:valid? true,
 :configs
 ({:model {:value 1},
   :last-op
   {:type :ok,
    :f :write,
    :value 1,
    :process 0,
    :time 15534211500,
    :index 37},
   :pending []}),
 :final-paths ()}


Everything looks good! ヽ(‘ー`)ノ
```

The last operation in this history was a write of `1`, and sure enough, the
checker's final value is also `1`. This history was linearizable.

## Multiple checkers

Checkers can render all kinds of output--as data structures, images, or
interactive visualizations. For instance, if we have `gnuplot` installed,
Jepsen can generate throughput and latency graphs for us. Let's use
`checker/compose` to run both a linearizability analysis, and generate
performance graphs.

```clj
         :checker (checker/compose
                    {:perf   (checker/perf)
                     :linear checker/linearizable})))
```

```bash
$ lein run test
...
$ open store/latest/latency-raw.png
```

We can also generate HTML visualizations of the history. Let's add the `jepsen.checker.timeline` namespace:

```clj
(ns jepsen.etcdemo
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [slingshot.slingshot :refer [try+]]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [tests :as tests]
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
```

And add that checker to the test:

```clj
          :checker (checker/compose
                     {:perf     (checker/perf)
                      :timeline (timeline/html)
                      :linear   checker/linearizable})
```

Now we can plot how different processes performed operations over time--which
ones were concurrent, which ones succeeded, failed, or crashed, and so on.

```bash
$ lein run test
...
$ open store/latest/timeline.html
```

Now that we've got a passing test, it's time to [introduce
failures](nemesis.md) into the system.
