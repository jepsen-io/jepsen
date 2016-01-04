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

`gen/seq` takes a sequence of generators and emits a single op from each one.
We use `cycle` to construct an infinite loop of sleep, start, sleep, stop, ...,
which ends once the time limit is up.

The network partition causes many operations to crash:

```clj
WARN  jepsen.core - Process 3 indeterminate
java.lang.IllegalMonitorStateException
```
or

```clj
INFO  jepsen.util - 7 :info :write  2 :timeout
```

... and so on. If we *know* an operation didn't take place we can make the
checker much more efficient (and detect more bugs!) by returning ops with
`:type :fail` instead of letting `client/invoke!` throw exceptions, but letting
every error crash the process is still legal.

If you run this test several times, you might notice an interesting result.
Sometimes--but not often--the test fails.

```clj
$ lein test
  ...
     :model {:msg "can't read 1 from register 3"}}]),
  ...
  :op
  {:type :ok,
   :f :read,
   :value 1,
   :process 1,
   :time 11443123716,
   :index 64}}
...
FAIL in (zk-test) (zookeeper_test.clj:7)
expected: (:valid? (:results (jepsen/run! (zk/zk-test "3.4.5+dfsg-2"))))
  actual: false
```

Knossos ran out of options: it thought the only legal value for the register
was `3`, but a process successfully read `1`. This happened on line 64 of the
history. When a linearizability failure occurs, Knossos will emit an SVG
diagram showing the problem--and we can read the history to see the op in more
detail.

```clj
$ open store/latest/linear.svg
$ open store/latest/history.txt
```

This is a case of a stale read. Avout *caches* reads, which means we might not
see the current value. This isn't a bug, really--Avout and ZK are behaving as
designed and documented, and we could just issue a `sync` command to ensure the
most recent value is visible--but as an illustrative example, let's pretend
it's a *real* bug in Zookeeper. How could we reproduce the failure more
reliably?

One option is to throw more ops at the system. We can lower the `gen/stagger`
parameter to increase the request rate, and lengthen the test duration:

```clj
         :generator (->> (gen/mix [r w cas])
                         (gen/stagger 1/10)
                         (gen/nemesis
                           (gen/seq (cycle [(gen/sleep 5)
                                            {:type :info, :f :start}
                                            (gen/sleep 5)
                                            {:type :info, :f :stop}])))
                         (gen/time-limit 60)
```

But this approach has limited utility--over time, as operation after operation
crashes, the linearizability checker builds up a pool of *every possible*
pending operation. If we see a read of 3, it's very likely there's a crashed
write of 3 outstanding we could use to satisfy it. Almost every history becomes
legal. Raising the request rate, paradoxically, can make the test *less
powerful*

The other consequence of crashed processes is exponentially large search spaces, which manifest as the checker grinding to a halt:

```clj
INFO  knossos.linear - :space 25 :cost 1.10E+34 :op {:type :ok, :f :read, :value 0, :process 12, :time 21364611211, :index 241}
```

Only 241 operations into the history, we've accrued 10^34 possible orders to
explore. So, we need *short* histories, but we want *lots of them*. That's
where `jepsen.independent` comes into play--but that's a discussion for another
time.

For now, take a break. Congratulations! You've written your first Jepsen test
from scratch!
