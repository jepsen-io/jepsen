# Nemeses

The nemesis is a special client, not bound to any particular node, which
introduces failures across the cluster. We'll require `jepsen.nemesis`, which
provides several built-in failure modes.

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
                    [nemesis    :as nemesis]
                    [tests      :as tests]
                    [util       :refer [timeout]]]
            [jepsen.os.debian   :as debian]
            [knossos.model      :as model]))
```

We'll pick a simple nemesis to start, and add it to the `:nemesis` key for the
test. This one partitions the network into two halves, selected randomly, when
it receives a `:start` op, and heals the network when it receives a `:stop`.

```clj
(defn zk-test
  [version]
  (assoc tests/noop-test
         :name    "zookeeper"
         :os      debian/os
         :db      (db version)
         :client  (client nil nil)
         :nemesis (nemesis/partition-random-halves)
         :generator (->> (gen/mix [r w cas])
                         (gen/stagger 1)
                         (gen/clients)
                         (gen/time-limit 15))
         :model   (model/cas-register 0)
         :checker (checker/compose
                    {:perf   (checker/perf)
                     :linear checker/linearizable})))
```

Like regular clients, the nemesis draws operations from the generator. Right
now our generator only emits ops to regular clients thanks to
`gen/clients`--we'll replace that with `gen/nemesis`, which splits off nemesis
ops into their own dedicated sub-generator.

```clj
         :generator (->> (gen/mix [r w cas])
                         (gen/stagger 1)
                         (gen/nemesis
                           (gen/seq (cycle [(gen/sleep 5)
                                            {:type :info, :f :start}
                                            (gen/sleep 5)
                                            {:type :info, :f :stop}])))
                         (gen/time-limit 15))
```
