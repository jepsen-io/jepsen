# Refining a test

Our test identified a fault, but it took some luck and clever guessing to
stumble upon it. It's time to refine our test, to make it faster, easier to
understand, and more powerful.

In order to analyze the history of a single key, Jepsen searches through every
permutation of concurrent operations, looking for a history that follows the
rules of a compare-and-set register. This means our search is exponential in
the number of concurrent operations at any given point in time.

Jepsen runs with a fixed number of worker threads, which would ordinarily limit
the number of concurrent operations too. However, when operations *crash*
(either by returning an `:info` result or throwing an exception), we abandon
that operation and let the thread move on to something new. It might be the
case that the crashed process' operation is still in-flight, and might be
executed by the database at some later time. This implies that crashed
operations are *concurrent for the entire remainder of the history.*

The more crashed operations, the more operations are concurrent by the end of
the history. That linear increase in concurrency is accompanied by an
exponential increase in verification time. Our first order of business is
reducing the number of crashed operations. We'll start with reads.

## Crashed reads

When an operation times out, we get a long stacktrace like

```
WARN [2017-03-31 19:12:46,969] jepsen worker 0 - jepsen.core Process 0 indeterminate
java.net.SocketTimeoutException: Read timed out
  at java.net.SocketInputStream.socketRead0(Native Method) ~[na:1.8.0_40]
  ...
```

... and that process' operation is converted to an `:info` message, because we
can't tell if it succeeded or failed. However, *idempotent* operations, like
reads, leave the state of the system unchanged. It doesn't *matter* whether
they succeed or fail, because the effects are equivalent. We can therefore
safely convert crashed reads to failed reads, and improve checker performance.

```clj
    (invoke! [this test op]
      (case (:f op)
        :read (try (let [value (-> conn
                                   (v/get "r" {:quorum? true})
                                   parse-long)]
                     (assoc op :type :ok, :value value))
                   (catch java.net.SocketTimeoutException e
                     (assoc op :type :fail, :error :timeout)))
        :write (do (v/reset! conn "r" (:value op))
                   (assoc op :type, :ok))
        :cas (try+
               (let [[value value'] (:value op)]
                 (assoc op :type (if (v/cas! conn "r" value value'
                                             {:prev-exist? true})
                                   :ok
                                   :fail)))
               (catch [:errorCode 100] _
                 (assoc op :type :fail, :error :not-found)))))
```

Better yet--we can get rid of all the exception stacktrace noise in the logs if
we catch socket timeouts on all three paths at once. We'll handle not-found
errors there too, even though they only happen on `:cas` ops--it keeps the code a
little cleaner.

```clj
    (invoke! [this test op]
      (try+
        (case (:f op)
          :read (let [value (-> conn
                                (v/get "r" {:quorum? true})
                                parse-long)]
                  (assoc op :type :ok, :value value))

          :write (do (v/reset! conn "r" (:value op))
                     (assoc op :type, :ok))

          :cas (let [[value value'] (:value op)]
                 (assoc op :type (if (v/cas! conn "r" value value'
                                             {:prev-exist? true})
                                   :ok
                                   :fail))))

        (catch java.net.SocketTimeoutException e
          (assoc op
                 :type (if (= :read (:f op)) :fail :info)
                 :error :timeout))

        (catch [:errorCode 100] e
          (assoc op :type :fail, :error :not-found))))
```

Now we get nice short timeout errors.

```bash
INFO [2017-03-31 19:34:47,351] jepsen worker 4 - jepsen.util 4  :info :cas  [4 4] :timeout
```

## Independent keys

We have a working test for a single linearizable key. However, sooner or later
processes *are* going to crash, and our concurrency will rise, slowing down the
analysis. We need a way to *bound* the length of a history on a particular key,
while still performing enough operations to observe concurrency errors.

Since operations on independent keys linearize independently, we can *lift* our
single-key test into one which operates on multiple keys. The
`jepsen.independent` namespace provides support.

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
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
```

We have a generator that emits operations on a single key, like `{:type :invoke,
:f :write, :value 3}`. We want to lift that to an operation that writes
*multiple* keys. Instead of `:value v`, we want `:value [key v]`.

```clj
          :generator (->> (independent/concurrent-generator
                            10
                            (range)
                            (fn [k]
                              (->> (gen/mix [r w cas])
                                   (gen/stagger 1/10)
                                   (gen/limit 100))))
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}
```

Our mix of reads, writes, and cas ops is still in there, but it's been wrapped
up in a *function*, which takes a key and returns a generator of values for
that particular key. We're using `concurrent-generator` to have 10 threads per
key, with keys taken from the infinite sequence of integers `(range)`, and
generators for those keys derived from `(fn [k] ...)`.

`concurrent-generator` changed the shape of our values from `v` to `[k v]`, so
we need to modify our client to understand how to read and write to different
keys.

```clj
    (invoke! [this test op]
      (let [[k v] (:value op)]
        (try+
          (case (:f op)
            :read (let [value (-> conn
                                  (v/get k {:quorum? true})
                                  parse-long)]
                    (assoc op :type :ok, :value (independent/tuple k value)))

            :write (do (v/reset! conn k v)
                       (assoc op :type, :ok))

            :cas (let [[value value'] v]
                   (assoc op :type (if (v/cas! conn k value value'
                                               {:prev-exist? true})
                                     :ok
                                     :fail))))

          (catch java.net.SocketTimeoutException e
            (assoc op
                   :type (if (= :read (:f op)) :fail :info)
                   :error :timeout))

          (catch [:errorCode 100] e
            (assoc op :type :fail, :error :not-found)))))
```

See how our hardcoded key `"r"` is gone? Now every key is parameterized by the
operation itself. Also note that where we modify the value--for instance, in
`:f :read`--we have to construct a special `independent/tuple` for the
key/value pair. Having a special datatype for tuples allows
`jepsen.independent` to split up the history later.

Finally, our checker thinks in terms of a single value--but we can turn that
into a checker that reasons about *independent* values, identified by keys.

```clj
          :checker (checker/compose
                     {:perf     (checker/perf)
                      :timeline (timeline/html)
                      :linear   (independent/checker checker/linearizable)})
```

Write one checker, get a family of n checkers for free! Maaaaagic!

```bash
$ lein run test --time-limit 30
...
ERROR [2017-03-31 19:51:28,300] main - jepsen.cli Oh jeez, I'm sorry, Jepsen broke. Here's why:
java.util.concurrent.ExecutionException: java.lang.AssertionError: Assert failed: This jepsen.independent/concurrent-generator has 5 threads to work with, but can only use 0 of those threads to run 0 concurrent keys with 10 threads apiece. Consider raising or lowering the test's :concurrency to a multiple of 10.
```

Aha. Our default concurrency is 5 threads, but we're asking for at least 10 in order to run a single key. Let's run 10 keys, using 100 threads.

```bash
$ lein run test --time-limit 30 --concurrency 100
...
142 :invoke :read [134 nil]
67  :invoke :read [133 nil]
66  :ok :read [133 1]
101 :ok :read [137 3]
181 :ok :write  [135 3]
116 :ok :read [131 3]
111 :fail :cas  [131 [0 0]]
151 :invoke :read [138 nil]
129 :ok :write  [130 2]
159 :ok :read [138 1]
64  :ok :write  [133 0]
69  :ok :cas  [133 [0 0]]
109 :ok :cas  [137 [4 3]]
89  :ok :read [135 1]
139 :ok :read [139 4]
19  :fail :cas  [131 [2 1]]
124 :fail :cas  [130 [4 4]]
```

Look at that! We can perform far more operations in a limited time window now. This helps us find bugs faster.

There are lots more techniques in Jepsen, but this is where our discussion
draws to a close. Thanks for reading!
