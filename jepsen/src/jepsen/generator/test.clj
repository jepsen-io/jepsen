(ns jepsen.generator.test
  "This namespace contains functions for testing generators. See the
  `jepsen.generator-test` namespace in the `test/` directory for a concrete
  example of how these functions can be used.

  NOTE: While the `simulate` function is considered stable at this point, the
  others might still be subject to change -- use with care and expect possible
  breakage in future releases."
  (:require [clojure.datafy :refer [datafy]]
            [dom-top.core :refer [assert+]]
            [jepsen [generator :as gen]
                    [history :as history]]
            [jepsen.generator.context :as ctx])
  (:import (io.lacuna.bifurcan Set)))

(def default-test
  "A default test map."
  {})

(defn n+nemesis-context
  "A context with n numeric worker threads and one nemesis."
  [n]
  (gen/context {:concurrency n}))

(def default-context
  "A default initial context for running these tests. Two worker threads, one
  nemesis."
  (n+nemesis-context 2))

(defn invocations
  "Only invokes, not returns"
  [history]
  (filter #(= :invoke (:type %)) history))

(defmacro with-fixed-rand-int
  "Rebinds rand-int to yield a deterministic series of random values.
  Definitely not threadsafe, but fine for tests I think."
  [seed & body]
  `(let [values#    (atom (gen/rand-int-seq ~seed))
         rand-int#  (fn [limit#]
                      (if (zero? limit#)
                        0
                        (mod (first (swap! values# next))
                             limit#)))]
     (with-redefs [rand-int rand-int#]
       ~@body)))

(def rand-seed
  "We need tests to be deterministic for reproducibility, but also
  pseudorandom. Changing this seed will force rewriting some tests, but it
  might be necessary for discovering edge cases."
  45100)

(defn simulate
  "Simulates the series of operations obtained from a generator, given a
  function that takes a context and op and returns the completion for that op.

  Strips out op :index fields--it's generally not as useful for testing."
  ([gen complete-fn]
   (simulate default-context gen complete-fn))
  ([ctx gen complete-fn]
   (with-fixed-rand-int rand-seed
     (loop [ops        []
            in-flight  [] ; Kept sorted by time
            gen        (gen/validate gen)
            ctx        ctx]
       ;(binding [*print-length* 3] (prn :invoking :gen gen))
       (let [[invoke gen'] (gen/op gen default-test ctx)]
         ; (prn :invoke invoke :in-flight in-flight)
         (if (nil? invoke)
           ; We're done
           (->> (into ops in-flight)
                (history/strip-indices))

           ; TODO: the order of updates for worker maps here isn't correct; fix
           ; it.
           (if (and (not= :pending invoke)
                    (or (empty? in-flight)
                        (<= (:time invoke) (:time (first in-flight)))))

             ; We have an invocation that's not pending, and that invocation is
             ; before every in-flight completion
             (let [thread    (gen/process->thread ctx (:process invoke))
                   ; Advance clock, mark thread as busy
                   ctx       (ctx/busy-thread
                               ctx
                               (max (:time ctx) (:time invoke))
                               thread)
                   ; Update the generator with this invocation
                   gen'      (gen/update gen' default-test ctx invoke)
                   ; Add the completion to the in-flight set
                   ;_         (prn :invoke invoke)
                   complete  (complete-fn ctx invoke)
                   in-flight (sort-by :time (conj in-flight complete))]
               (recur (conj ops invoke) in-flight gen' ctx))

             ; We need to complete something before we can apply the next
             ; invocation.
             (let [op     (first in-flight)
                   _      (assert+ op
                                   {:type :generator-pending-but-nothing-in-flight
                                    :gen gen'
                                    :ctx (datafy ctx)})
                   thread (gen/process->thread ctx (:process op))
                   ; Advance clock, mark thread as free
                   ctx    (ctx/free-thread ctx (:time op) thread)
                   ; Update generator with completion
                   gen'   (gen/update gen default-test ctx op)
                   ; Update worker mapping if this op crashed
                   ctx    (if (or (= :nemesis thread) (not= :info (:type op)))
                            ctx
                            (ctx/with-next-process ctx thread))]
               (recur (conj ops op) (rest in-flight) gen' ctx)))))))))

(defn quick-ops
  "Simulates the series of ops obtained from a generator where the
  system executes every operation perfectly, immediately, and with zero
  latency."
  ([gen]
   (quick-ops default-context gen))
  ([ctx gen]
   (simulate ctx gen (fn [ctx invoke] (assoc invoke :type :ok)))))

(defn quick
  "Like quick-ops, but returns just invocations."
  ([gen]
   (quick default-context gen))
  ([ctx gen]
   (invocations (quick-ops ctx gen))))

(def perfect-latency
  "How long perfect operations take"
  10)

(defn perfect*
  "Simulates the series of ops obtained from a generator where the system
  executes every operation successfully in 10 nanoseconds. Returns full
  history."
  ([gen]
   (perfect* default-context gen))
  ([ctx gen]
   (simulate ctx gen
             (fn [ctx invoke]
               (-> invoke
                   (assoc :type :ok)
                   (update :time + perfect-latency))))))

(defn perfect
  "Simulates the series of ops obtained from a generator where the system
  executes every operation successfully in 10 nanoseconds. Returns only
  invocations."
  ([gen]
   (perfect default-context gen))
  ([ctx gen]
   (invocations (perfect* ctx gen))))

(defn perfect-info
  "Simulates the series of ops obtained from a generator where every operation
  crashes with :info in 10 nanoseconds. Returns only invocations."
  ([gen]
   (perfect-info default-context gen))
  ([ctx gen]
   (invocations
     (simulate ctx gen
               (fn [ctx invoke]
                 (-> invoke
                     (assoc :type :info)
                     (update :time + perfect-latency)))))))

(defn imperfect
  "Simulates the series of ops obtained from a generator where threads
  alternately fail, info, then ok, and repeat, taking 10 ns each. Returns
  invocations and completions."
  ([gen]
   (imperfect default-context gen))
  ([ctx gen]
   (let [state (atom {})]
     (simulate ctx gen
               (fn [ctx invoke]
                 (let [t (gen/process->thread ctx (:process invoke))]
                   (-> invoke
                       (assoc :type (get (swap! state update t {nil   :fail
                                                                :fail :info
                                                                :info :ok
                                                                :ok   :fail})
                                         t))
                       (update :time + perfect-latency))))))))
