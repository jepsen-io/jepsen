(ns jepsen.tests.causal
  (:refer-clojure :exclude [test])
  (:require [jepsen [checker :as checker]
                    [generator :as gen]
                    [history :as h]
                    [independent :as independent]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]))

(defprotocol Model
  (step [model op]))

(defrecord Inconsistent [msg]
  Model
  (step [this op] this)

  Object
  (toString [this] msg))

(defn inconsistent
  "Represents an invalid termination of a model; e.g. that an operation could
  not have taken place."
  [msg]
  (Inconsistent. msg))

(defn inconsistent?
  "Is a model inconsistent?"
  [model]
  (instance? Inconsistent model))

(defrecord CausalRegister [value counter last-pos]
  Model
  (step [r op]
    (let [c (inc counter)
          v'   (:value op)
          pos  (:position op)
          link (:link op)]
      (if-not (or (= link :init)
                  (= link last-pos))
        (inconsistent (str "Cannot link " link
                                 " to last-seen position " last-pos))
        (condp = (:f op)
          :write (cond
                   ;; Write aligns with next counter, OK
                   (= v' c)
                   (CausalRegister. v' c pos)

                   ;; Attempting to write an unknown value
                   (not= v' c)
                   (inconsistent (str "expected value " c
                                            " attempting to write "
                                            v' " instead")))

          :read-init  (cond
                        ;; Read a non-0 value from a freshly initialized register
                        (and (=    0 counter)
                             (not= 0 v'))
                        (inconsistent (str "expected init value 0, read " v'))

                        ;; Read the expected value of the register,
                        ;; update the last known position
                        (or (nil? v')
                            (= value v'))
                        (CausalRegister. value counter pos)

                        ;; Read a value that we haven't written
                        true (inconsistent (str "can't read " v'
                                                      " from register " value)))

          :read  (cond
                   ;; Read the expected value of the register,
                   ;; update the last known position
                   (or (nil? v')
                       (= value v'))
                   (CausalRegister. value counter pos)

                   ;; Read a value that we haven't written
                   true (inconsistent (str "can't read " v'
                                                 " from register " value)))))))
  Object
  (toString [this] (pr-str value)))

(defn causal-register []
  (CausalRegister. 0 0 nil))

(defn check
  "A series of causally consistent (CC) ops are a causal order (CO). We issue a
  CO of 5 read (r) and write (w) operations (r w r w r) against a register
  (key). All operations in this CO must appear to execute in the order provided
  by the issuing site (process). We also look for anomalies, such as unexpected
  values"
  [model]
  (reify checker/Checker
    (check [this test history opts]
      (let [completed (h/oks history)]
        (loop [s model
               history completed]
          (if (empty? history)
            ;; We've checked every operation in the history
            {:valid? true
             :model s}

            ;; checking checking checking...
            (let [op (first history)
                  s' (step s op)]
              (if (inconsistent? s')
                {:valid? false
                 :error (:msg s')}
                (recur s' (rest history))))))))))

;; Generators
(defn r   [_ _] {:type :invoke, :f :read})
(defn ri  [_ _] {:type :invoke, :f :read-init})
(defn cw1 [_ _] {:type :invoke, :f :write, :value 1})
(defn cw2 [_ _] {:type :invoke, :f :write, :value 2})

(defn test
  [opts]
  {:checker (independent/checker (check (causal-register)))
   :generator (->> (independent/concurrent-generator
                     1
                     (range)
                     (fn [k] [ri cw1 r cw2 r]))
                   (gen/stagger 1)
                   (gen/nemesis
                     (cycle [(gen/sleep 10)
                             {:type :info, :f :start}
                             (gen/sleep 10)
                             {:type :info, :f :stop}]))
                   (gen/time-limit (:time-limit opts)))})
