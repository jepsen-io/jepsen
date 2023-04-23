# Introducing Faults

The nemesis is a special client, not bound to any particular node, which
introduces failures across the cluster. We'll require `jepsen.nemesis`, which
provides several built-in failure modes.

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]
            [verschlimmbesserung.core :as v]))
```

We'll pick a simple nemesis to start, and add it to the `:nemesis` key for the
test. This one partitions the network into two halves, selected randomly, when
it receives a `:start` op, and heals the network when it receives a `:stop`.

```clj

(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "etcd"
          :os              debian/os
          :db              (db "v3.1.5")
          :client          (Client. nil)
          :nemesis         (nemesis/partition-random-halves)
          :checker         (checker/compose
                             {:perf   (checker/perf)
                              :linear (checker/linearizable
                                        {:model     (model/cas-register)
                                         :algorithm :linear})
                              :timeline (timeline/html)})
          :generator       (->> (gen/mix [r w cas])
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))}))
```

Like regular clients, the nemesis draws operations from the generator. Right
now our generator only emits ops to regular clients--the nemesis just gets
`nil`, which tells it there's nothing to do. We'll replace that with a
dedicated generator for nemesis operations. We're also going to increase the time limit, so we have enough time to see the nemesis take effect.

```clj
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis
                            (cycle [(gen/sleep 5)
                              {:type :info, :f :start}
                              (gen/sleep 5)
                              {:type :info, :f :stop}]))
                          (gen/time-limit 30))}
```

Clojure sequences can act as generators, so we can use regular Clojure
functions to construct them. Here, we use `cycle` to construct an infinite loop
of sleep, start, sleep, stop, ..., which ends once the time limit is up.

The network partition causes some operations to crash:

```clj
WARN [2018-02-02 15:54:53,380] jepsen worker 1 - jepsen.core Process 1 crashed
java.net.SocketTimeoutException: Read timed out
```

... and so on. If we *know* an operation didn't take place we can make the
checker more efficient (and detect more bugs!) by returning ops with `:type
:fail` instead of letting `client/invoke!` throw exceptions, but letting every
error crash the process is still safe: jepsen's checkers understand that a
crashed operation may or may not take place.

## Finding a bug

We've hardcoded a 30 second time limit into our test, but it'd be nice if we
could control that at the command line. Jepsen's CLI kit provides a
`--time-limit` switch, which is passed to `etcd-test` as `:time-limit`, in the
options map. Let's hook that up now:

```clj
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}
```

```bash
$ lein run test --time-limit 60
...
```

Now that we can run tests for shorter or longer, let's speed up the request rate. If we take too long between requests, we won't have a chance to see interesting behaviors. Let's try a tenth of a second between requests:

```clj
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1/50)
                          (gen/nemesis
                           (cycle [(gen/sleep 5)
                            {:type :info, :f :start}
                            (gen/sleep 5)
                            {:type :info, :f :stop}]))
                          (gen/time-limit (:time-limit opts)))}
```

If you run this test a few times, you might notice an interesting result.
Sometimes, it fails!

```clj
$ lein run test --test-count 10
...
     :model {:msg "can't read 3 from register 4"}}]
...
Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
```

Knossos ran out of options: it thought the only legal value for the register
was 4, but a process successfully read 3. When a linearizability failure
occurs, Knossos will emit an SVG diagram showing the problem--and we can read
the history to see the op in more detail.

```clj
$ open store/latest/linear.svg
$ open store/latest/history.txt
```

This is a case of a stale read: we saw a value from the *past*, despite more
recent writes having completed. This occurs because etcd allows us to read the
local state of any replica, without going through consensus to ensure we have
the most recent state.

## Read consistency

The etcd docs claim "etcd ensures linearizability for all [operations other
than watches] by default." This is clearly not the case--and indeed, buried in
the [v2 API docs](https://coreos.com/etcd/docs/latest/v2/api.html) is this
unobtrusive note:

> If you want a read that is fully linearized you can use a quorum=true GET. The read will take a very similar path to a write and will have a similar speed. If you are unsure if you need this feature feel free to email etcd-dev for advice.

Aha! So we need to use *quorum* reads. Verschlimmbesserung has an option for
that:

```clj
    (invoke! [this test op]
      (case (:f op)
        :read (let [value (-> conn
                              (v/get "foo" {:quorum? true})
                              parse-long-nil)]
                (assoc op :type :ok, :value value))
      ...
```

Introducing quorum reads makes our tests pass!

```bash
$ lein run test
...
Everything looks good! ヽ(‘ー`)ノ
```

Congratulations! You've written your first successful Jepsen test. This is the
same issue I identified in
[2014](https://aphyr.com/posts/316-jepsen-etcd-and-consul), and dialogue with
the etcd team led them to introduce the *quorum* read option.

Take a quick break! You've earned it! Then, if you like, we can move onto [refining the test](06-refining.md).
