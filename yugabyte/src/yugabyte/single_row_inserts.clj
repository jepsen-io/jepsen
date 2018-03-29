(ns yugabyte.single-row-inserts
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
             [db        :as db]
             [util      :as util :refer [meh timeout]]
             [control   :as c :refer [| lit]]
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [nemesis   :as nemesis]
             [store     :as store]
             [report    :as report]
             [tests     :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control [net :as net]
             [util :as net/util]]
            [jepsen.os.debian :as debian]
            [knossos.core :as knossos]
            [knossos.op :as op]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.query :refer :all]
            [clojurewerkz.cassaforte.policies :refer :all]
            [clojurewerkz.cassaforte.cql :as cql]
            [yugabyte.core :refer :all]
            )
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core ConsistencyLevel)
           (com.datastax.driver.core.exceptions UnavailableException
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

(defrecord CQLRowInsertClient [conn]
  client/Client
  (setup! [_ test node]
    (locking setup-lock
      (let [conn (cassandra/connect (->> test :nodes (map name)) {:protocol-version 3})]
        (cql/create-keyspace conn "jepsen_keyspace"
                             (if-not-exists)
                             (with {:replication
                                    {:class "SimpleStrategy"
                                     :replication_factor 3}}))
        (cql/use-keyspace conn "jepsen_keyspace")
        (cql/create-table conn "kv_pairs"
                          (if-not-exists)
                          (column-definitions {:id :int
                                               :val :int
                                               :primary-key [:id]}))
        (->CQLRowInsertClient conn))))
  (invoke! [this test op]
    (case (:f op)
      :write (try (cql/insert conn "kv_pairs" (mk-pair (:value op)))
                (assoc op :type :ok)
                (catch UnavailableException e
                  (assoc op :type :fail :value (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :timed-out))
                (catch NoHostAvailableException e
                  (info "All nodes are down - sleeping 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :value (.getMessage e))))
      :read (try (wait-for-recovery 30 conn)
                 (let [value (->> (cql/select conn "kv_pairs"))]
                   (assoc op :type :ok :value value))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :value (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :fail :value :timed-out))
                 (catch NoHostAvailableException e
                   (info "All nodes are down - sleeping 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :value (.getMessage e))))))
  (teardown! [_ _]
    (info "Tearing down client with conn" conn)
    (cassandra/disconnect! conn)))

(defn test
  [opts]
  (yugabyte-test
    (merge opts
         {:name "Single row inserts"
          :client (CQLRowInsertClient. nil)
          :generator (gen/phases
            (->> w
                 (gen/stagger 1)
                 (gen/nemesis nil)
                 (gen/time-limit 5))
            (gen/clients (gen/once r))
          )
          :checker (checker/compose {:perf (checker/perf)
                                     :details (check-inserts)})
         })))
