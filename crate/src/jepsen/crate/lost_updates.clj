(ns jepsen.crate.lost-updates
  "Implements a map of keys to sets of integers, and adds integers to each set
  by reading and writing back an updated element list with a _version check.
  Final reads should have all successfully inserted values."
  (:refer-clojure :exclude [test])
  (:require [jepsen [core         :as jepsen]
                    [db           :as db]
                    [checker      :as checker]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [net          :as net]
                    [tests        :as tests]
                    [util         :as util :refer [meh
                                                   timeout
                                                   with-retry]]
                    [os           :as os]]
            [jepsen.os.debian     :as debian]
            [jepsen.checker.timeline :as timeline]
            [jepsen.crate.core    :as c]
            [cheshire.core        :as json]
            [clojure.string       :as str]
            [clojure.java.jdbc    :as j]
            [clojure.java.io      :as io]
            [clojure.java.shell   :refer [sh]]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [knossos.op           :as op])
  (:import (io.crate.shade.org.postgresql.util PSQLException)))

(defrecord LostUpdatesClient [tbl-created? conn]
  client/Client

  (setup! [this test])

  (open! [this test node]
    (let [conn (c/jdbc-client node)]
      (info node "Connected")
      ;; Everyone's gotta block until we've made the table.
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (c/with-conn [c conn]
            (j/execute! c ["drop table if exists sets"])
            (info node "Creating table sets")
            (j/execute! c
                        ["create table if not exists sets (
                         id       integer primary key,
                         elements string INDEX OFF STORAGE WITH (columnstore = false))"])
            (j/execute! c
                        ["alter table sets
                         set (number_of_replicas = \"0-all\")"]))))

      (assoc this :conn conn)))

  (invoke! [this test op]
    (c/with-exception->op op
      (c/with-conn [c conn]
        (c/with-timeout
          (c/with-txn [c c]
            (let [[k v] (:value op)]
              (c/with-errors op
                (case (:f op)
                  :read (->> (c/query c ["select elements
                                         from sets where id = ?" k])
                             first
                             :elements
                             json/parse-string
                             set
                             (independent/tuple k)
                             (assoc op :type :ok, :value))

                  :add (if-let [cur (first (c/query c ["select elements, _version
                                                       from sets where id = ?"
                                                       k]))
                                ]
                         (let [els' (-> (:elements cur)
                                        json/parse-string
                                        (conj v)
                                        json/generate-string)
                               _ (assert (number? (:_version cur)))
                               row_count (first
                                           (j/execute! c ["update sets set elements = ?
                                                          where id = ? and _version = ?"
                                                          els' k (:_version cur)]
                                                       {:timeout c/timeout-delay}))]
                           (case row_count
                             0 (assoc op :type :fail)
                             1 (assoc op :type :ok)
                             2 (assoc op :type :info
                                      :error (str "Updated " row_count
                                                  " rows!?"))))
                         (let [els' (json/generate-string [v])]
                           (j/execute! c ["insert into sets (id, elements)
                                          values (?, ?)"
                                          k els']
                                       {:timeout c/timeout-delay})
                           (assoc op :type :ok)))))))))))

  (close! [this test])

  (teardown! [this test]
    ))

(defn r [] {:type :invoke, :f :read, :value nil})
(defn w []
  (->> (iterate inc 0)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn test
  [opts]
  (let [time-limit      380
        quiescence-time 20
        nemesis-time    (- time-limit quiescence-time)]
    (merge tests/noop-test
           {:name    "lost-updates"
            :os      debian/os
            :db      (c/db (:tarball opts))
            :client  (LostUpdatesClient. (atom false) nil) 
            :checker (checker/compose
                       {:perf (checker/perf)
                        :set  (independent/checker (checker/set))})
            :concurrency 100
            :nemesis (nemesis/partition-random-halves)
            :generator (->> (independent/concurrent-generator
                              10
                              (range)
                              (fn [id]
                                (gen/phases
                                  (gen/time-limit nemesis-time
                                                  (gen/delay 1/100 (w)))
                                  ; Wait for quiescence
                                  (gen/sleep quiescence-time)
                                  (gen/each
                                    (gen/once (r))))))
                            (gen/time-limit time-limit)
                            (gen/nemesis
                              (gen/phases
                                (->> (gen/seq (cycle [(gen/sleep 120)
                                                      {:type :info, :f :start}
                                                      (gen/sleep 120)
                                                      {:type :info, :f :stop}]))
                                     (gen/time-limit nemesis-time))
                                (gen/once {:type :info, :f :stop})
                                (gen/log "Waiting for quiescence")
                                (gen/sleep quiescence-time))))}
           opts)))
