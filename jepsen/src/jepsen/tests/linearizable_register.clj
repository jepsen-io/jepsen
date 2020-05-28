(ns jepsen.tests.linearizable-register
  "Common generators and checkers for linearizability over a set of independent
  registers. Clients should understand three functions, for writing a value,
  reading a value, and compare-and-setting a value from v to v'. Reads receive
  `nil`, and replace it with the value actually read.

      {:type :invoke, :f :write, :value [k v]}
      {:type :invoke, :f :read,  :value [k nil]}
      {:type :invoke, :f :cas,   :value [k [v v']]}"
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [independent :as independent]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]))

(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn test
  "A partial test, including a generator, model, and checker. You'll need to
  provide a client. Options:

    :nodes            A set of nodes you're going to operate on. We only care
                      about the count, so we can figure out how many workers
                      to use per key.
    :model            A model for checking. Default is (model/cas-register).
    :per-key-limit    Maximum number of ops per key.
    :process-limit    Maximum number of processes that can interact with a
                      given key. Default 20."
  [opts]
  {:checker (independent/checker
              (checker/compose
               {:linearizable (checker/linearizable
                                {:model (:model opts (model/cas-register))})
                :timeline     (timeline/html)}))
   :generator (let [n (count (:nodes opts))]
                (independent/concurrent-generator
                  (* 2 n)
                  (range)
                  (fn [k]
                    (cond->> (gen/reserve n r (gen/mix [w cas cas]))
                      ; We randomize the limit a bit so that over time, keys
                      ; become misaligned, which prevents us from lining up
                      ; on Significant Event Boundaries.
                      (:per-key-limit opts)
                      (gen/limit (* (+ (rand 0.1) 0.9)
                                    (:per-key-limit opts 20)))

                      true
                      (gen/process-limit (:process-limit opts 20))))))})
