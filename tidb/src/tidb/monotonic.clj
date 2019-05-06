(ns tidb.monotonic
  "Establishes a collection of integers identified by keys. Monotonically
  increments individual keys via read-write transactions, and reads keys in
  small groups. We verify that the order of transactions implied by each key
  are mutually consistent; e.g. no transaction can observe key x increase, but
  key y decrease."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]
            [tidb [sql :as c :refer :all]
                  [txn :as txn]]))

(defn read-key
  "Read a specific key's value from the table. Missing values are represented
  as -1."
  [c test k]
  (-> (c/query c [(str "select (val) from cycle where "
                       (if (:use-index test) "pk" "sk") " = ?")
                  k])
      first
      (:val -1)))

(defn read-keys
  "Read several keys values from the table, returning a map of keys to values."
  [c test ks]
  (->> (map (partial read-key c test) ks)
       (zipmap ks)
       (into (sorted-map))))
  ;(zipmap ks (map (partial read-key c test) ks)))

(defrecord IncrementClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (c/execute! conn ["create table if not exists cycle
                        (pk  int not null primary key,
                         sk  int not null,
                         val int)"])
      (when (:use-index test)
        (c/create-index! conn ["create index cycle_sk_val on cycle (sk, val)"]))))

  (invoke! [this test op]
    (c/with-txn op [c conn]
    ;(let [c conn]
      (case (:f op)
        :read (let [v (read-keys c test (shuffle (keys (:value op))))]
                (assoc op :type :ok, :value v))
        :inc (let [k (:value op)]
               (if (:update-in-place test)
                 ; Update directly
                 (do (when (= [0] (c/execute!
                                    c [(str "update cycle set val = val + 1"
                                            " where pk = ?") k]))
                       ; That failed; insert
                       (c/insert! c "cycle" {:pk k, :sk k, :val 0}))
                     ; We can't place any constraints on the values since we
                     ; didn't read anything
                     (assoc op :type :ok, :value {}))

                 ; Update via separate r/w
                 (let [v (read-key c test k)]
                   (if (= -1 v)
                     (c/insert! c "cycle" {:pk k, :sk k, :val 0})
                     (c/update! c "cycle" {:val (inc v)},
                                [(str (if (:use-index test) "sk" "pk") " = ?")
                                 k]))
                   ; The monotonic value constraint isn't actually enough to
                   ; capture all the ordering dependencies here: an increment
                   ; from x->y must fall after every read of x, and before
                   ; every read of y, but the monotonic order relation can only
                   ; enforce one of those. We'll return the written value here.
                   ; Still better than nothing.
                   (assoc op :type :ok :value {k (inc v)})))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn reads [key-count]
  (fn [] {:type  :invoke
          :f     :read
          :value (-> (range key-count)
                     ;util/random-nonempty-subset
                     (zipmap (repeat nil)))}))

(defn incs [key-count]
  (fn [] {:type :invoke,
          :f :inc
          :value (rand-int key-count)}))

(defn inc-workload
  [opts]
  (let [key-count 8]
    {:client (IncrementClient. nil)
     :checker (checker/compose
                {:cycle (cycle/checker
                          (cycle/combine cycle/monotonic-key-graph
                                         cycle/realtime-graph))
                 :timeline (timeline/html)})
     :generator (->> (gen/mix [(incs key-count)
                               (reads key-count)]))}))

(defn wr-txns
  "A lazy sequence of write and read transactions over a pool of n numeric
  keys; every write is unique per key. Options:

    :key-count            Number of distinct keys
    :min-txn-length       Minimum number of operations per txn
    :max-txn-length       Maximum number of operations per txn
    :max-writes-per-key   Maximum number of operations per key"
  ([opts]
   (wr-txns opts {:active-keys (vec (range (:key-count opts)))}))
  ([opts state]
   (lazy-seq
     (let [min-length           (:min-txn-length opts 0)
           max-length           (:max-txn-length opts 2)
           max-writes-per-key   (:max-writes-per-key opts 32)
           key-count            (:key-count opts 2)
           length               (+ min-length (rand-int (- (inc max-length)
                                                           min-length)))
           [txn state] (loop [length  length
                              txn     []
                              state   state]
                         (let [active-keys (:active-keys state)]
                           (if (zero? length)
                             ; All done!
                             [txn state]
                             ; Add an op
                             (let [f (rand-nth [:r :w])
                                   k (rand-nth active-keys)
                                   v (when (= f :w) (get state k 1))]
                               (if (and (= :w f)
                                        (< max-writes-per-key v))
                                 ; We've updated this key too many times!
                                 (let [i  (.indexOf active-keys k)
                                       k' (inc (reduce max active-keys))
                                       state' (update state :active-keys
                                                      assoc i k')]
                                   (recur length txn state'))
                                 ; Key is valid, OK
                                 (let [state' (if (= f :w)
                                                (assoc state k (inc v))
                                                state)]
                                   (recur (dec length)
                                          (conj txn [f k v])
                                          state')))))))]
       (cons txn (wr-txns opts state))))))

(defn txn-workload
  [opts]
  {:client  (txn/client {:val-type "int"})
   :checker (cycle/checker
              (cycle/combine cycle/wr-graph
                             cycle/realtime-graph))
   :generator (->> (wr-txns {:min-txn-length 2, :max-txn-length 5})
                   (map (fn [txn] {:type :invoke, :f :txn, :value txn}))
                   gen/seq)})

(defn append-client
  "Wraps a TxnClient, translating string lists back into integers."
  [client]
  (reify client/Client
    (open! [this test node]
      (append-client (client/open! client test node)))

    (setup! [this test]
      (append-client (client/setup! client test)))

    (invoke! [this test op]
      (let [op' (client/invoke! client test op)
            txn' (mapv (fn [[f k v :as mop]]
                         (if (= f :r)
                           ; Rewrite reads to convert "1,2,3" to [1 2 3].
                           [f k (when v (mapv #(Long/parseLong %)
                                              (str/split v #",")))]
                           mop))
                       (:value op'))]
        (assoc op' :value txn')))

    (teardown! [this test]
      (client/teardown! client test))

    (close! [this test]
      (client/close! client test))))

(defn append-txns
  "Like wr-txns, we just rewrite writes to be appends."
  [opts]
  (->> (wr-txns opts)
       (map (partial mapv (fn [[f k v]] [(case f :w :append f) k v])))))

(defn append-workload
  [opts]
  {:client (append-client (txn/client {:val-type "text"}))
   :generator (->> (append-txns {:min-txn-length      1
                                 :max-txn-length      4
                                 :key-count           5
                                 :max-writes-per-key  16})
                   (map (fn [txn] {:type :invoke, :f :txn, :value txn}))
                   gen/seq)
   :checker (append/checker {:anomalies         [:G-single]
                             :additional-graphs [cycle/realtime-graph]})})
