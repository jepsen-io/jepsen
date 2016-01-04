# Adding a checker

With our generator and clients performing operations, we've got a history to
analyze for correctness. Jepsen uses a *model* to represent the abstract
behavior of a system, and a *checker* to verify whether the history conforms to
that model. We'll require `knossos.model` and `jepsen.checker`:

```clj
(ns jepsen.zookeeper
  (:require [avout.core         :as avout]
            [clojure.tools.logging :refer :all]
            [clojure.java.io    :as io]
            [clojure.string     :as str]
            [jepsen [db         :as db]
                    [checker    :as checker]
                    [client     :as client]
                    [control    :as c]
                    [generator  :as gen]
                    [tests      :as tests]
                    [util       :refer [timeout]]]
            [jepsen.os.debian   :as debian]
            [knossos.model      :as model]))
```

In our Zookeeper example, we're modeling a single register with reads, writes,
and compare-and-set ops. The Knossos library provides a model for us:
`cas-register`, which we'll initialize with a value of `0` to match our client.

To analyze the history, we'll specify a `:checker` for the test.
`checker/linearizable` uses the Knossos linearizability checker to verify that
every operation appears take place atomically between its invocation and
completion. It'll use the test's `:model` to specify how the system *should*
behave.

```clj
(defn zk-test
  [version]
  (assoc tests/noop-test
         :name    "zookeeper"
         :os      debian/os
         :db      (db version)
         :client  (client nil nil)
         :generator (->> (gen/mix [r w cas])
                         (gen/stagger 1)
                         (gen/clients)
                         (gen/time-limit 15))
         :model   (model/cas-register 0)
         :checker checker/linearizable))
```

Running the test, we can confirm the checker's results:

```bash
$ lein test
...
INFO  jepsen.util - 0 :invoke :read nil
INFO  jepsen.util - 0 :ok :read 2
INFO  jepsen.core - Worker 0 done
INFO  jepsen.util - 1 :invoke :cas  [3 1]
INFO  jepsen.util - 1 :fail :cas  [3 1]
INFO  jepsen.core - Worker 1 done
...
INFO  jepsen.core - Everything looks good! ヽ(‘ー`)ノ

{:valid? true,
 :configs ({:model {:value 2}, :pending []}),
 :final-paths ()}

```

The last successfully read value was `2`, and sure enough, the checker's final
state was `2`--this history was linearizable.

We can compose multiple checkers together! For instance, if we have `gnuplot`
installed, Jepsen can emit throughput and latency graphs for us.

```clj
         :checker (checker/compose
                    {:perf   (checker/perf)
                     :linear checker/linearizable})))
```

```bash
$ lein test
...
$ open store/latest/latency-raw.png
```

Now that we've got a passing test, it's time to [introduce
failures](nemesis.md) into the system.
