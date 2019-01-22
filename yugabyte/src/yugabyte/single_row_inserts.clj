(ns yugabyte.single-row-inserts
  (:require [clojure [pprint :refer :all]
                     [set :as set]]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [util :as util :refer [meh timeout]]]
            [jepsen.checker.timeline :as timeline]
            [knossos.op :as op]
            [clojurewerkz.cassaforte [client :as cassandra]
                                     [query :refer :all]
                                     [policies :refer :all]
                                     [cql :as cql]]
            [yugabyte.core :refer :all]
            )
  (:import (com.datastax.driver.core.exceptions UnavailableException
                                                OperationTimedOutException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defn mk-pair
  [x]
  {:id x :val x}
)

(defn check-inserts
  "Given a set of :add operations followed by a final :read, verifies that every successfully added
  row is present in the read, and that the read contains only rows for which an add was attempted,
  and that all rows are unique."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [attempts (->> history
                          (r/filter op/invoke?)
                          (r/filter #(= :write (:f %)))
                          (r/map (comp mk-pair :value))
                          (into #{})
                     )
            adds (->> history
                      (r/filter op/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            fails (->> history
                       (r/filter op/fail?)
                       (r/filter #(= :add (:f %)))
                       (r/map :value)
                       (into #{}))
            unsure (->> history
                        (r/filter op/info?)
                        (r/filter #(= :add (:f %)))
                        (r/map :value)
                        (into #{}))
            final-read-l (->> history
                              (r/filter op/ok?)
                              (r/filter #(= :read (:f %)))
                              (r/map :value)
                              (reduce (fn [_ x] x) nil))]
        (if-not final-read-l
          {:valid? :unknown
           :error  "Set was never read"}

          (let [final-read  (set final-read-l)

                dups        (into [] (for [[id freq] (frequencies final-read-l)
                                           :when (> freq 1)]
                                       id))

                ;;The OK set is every read value which we added successfully
                ok          (set/intersection final-read adds)

                ;; Unexpected records are those we *never* attempted.
                unexpected  (set/difference final-read attempts)

                ;; Revived records are those that were reported as failed and
                ;; still appear.
                revived     (set/intersection final-read fails)

                ;; Lost records are those we definitely added but weren't read
                lost        (set/difference adds final-read)

                ;; Recovered records are those where we didn't know if the add
                ;; succeeded or not, but we found them in the final set.
                recovered   (set/intersection final-read unsure)]

            {:valid?          (and (empty? lost)
                                   (empty? unexpected)
                                   (empty? dups)
                                   (empty? revived))
             :duplicates      dups
             :ok              (util/integer-interval-set-str ok)
             :lost            (util/integer-interval-set-str lost)
             :unexpected      (util/integer-interval-set-str unexpected)
             :recovered       (util/integer-interval-set-str recovered)
             :revived         (util/integer-interval-set-str revived)
             :ok-frac         (util/fraction (count ok) (count attempts))
             :revived-frac    (util/fraction (count revived) (count fails))
             :unexpected-frac (util/fraction (count unexpected) (count attempts))
             :lost-frac       (util/fraction (count lost) (count attempts))
             :recovered-frac  (util/fraction (count recovered) (count attempts))}))))))

(def table-name "kv_pairs")

(defrecord CQLRowInsertClient [conn]
  client/Client
  (open! [this test node]
         (info "Opening connection")
  (assoc this :conn (cassandra/connect (->> test :nodes (map name))
                                       {:protocol-version 3
                                        :retry-policy (retry-policy :no-retry-on-client-timeout)})))
  (setup! [this test]
    (locking setup-lock
      (cql/create-keyspace conn keyspace
                           (if-not-exists)
                           (with {:replication
                                  {"class" "SimpleStrategy"
                                   "replication_factor" 3}}))
      (cql/use-keyspace conn keyspace)
      (cql/create-table conn "kv_pairs"
                        (if-not-exists)
                        (column-definitions {:id :int
                                             :val :int
                                             :primary-key [:id]}))
      (->CQLRowInsertClient conn)))
  (invoke! [this test op]
    (case (:f op)
      :write (try
                (cql/insert-with-ks conn keyspace table-name (mk-pair (:value op)))
                (assoc op :type :ok)
                (catch UnavailableException e
                  (assoc op :type :fail :error (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :error :write-timed-out))
                (catch OperationTimedOutException e
                  (assoc op :type :info :error :client-timed-out))
                (catch NoHostAvailableException e
                  (info "All nodes are down - sleeping 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :error (.getMessage e))))
      :read (try (wait-for-recovery 30 conn)
                 (let [value (->> (cql/select-with-ks conn keyspace table-name))]
                   (assoc op :type :ok :value value))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :error (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :fail :error :read-timed-out))
                 (catch OperationTimedOutException e
                   (assoc op :type :fail :error :client-timed-out))
                 (catch NoHostAvailableException e
                   (info "All nodes are down - sleeping 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :error (.getMessage e))))))
   (teardown! [this test])
   (close! [this test]
    (info "Closing client with conn" conn)
    (cassandra/disconnect! conn)))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 500)})

(defn test
  [opts]
  (yugabyte-test
    (merge opts
         {:name "single-row-inserts"
          :client (CQLRowInsertClient. nil)
          :client-generator (->> w
                                 (gen/stagger 1))
          :client-final-generator (gen/once r)
          :checker (checker/compose {:perf (checker/perf)
                                     :timeline (timeline/html)
                                     :details (check-inserts)})
         })))
