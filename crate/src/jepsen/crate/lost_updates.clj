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
            [jepsen.crate         :as c]
            [cheshire.core        :as json]
            [clojure.string       :as str]
            [clojure.java.io      :as io]
            [clojure.java.shell   :refer [sh]]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [knossos.op           :as op])
  (:import (io.crate.client CrateClient)
           (io.crate.action.sql SQLActionException
                                SQLResponse
                                SQLRequest)
           (io.crate.shade.org.elasticsearch.client.transport
             NoNodeAvailableException)))

(defn client
  ([] (client nil))
  ([conn]
   (let [initialized? (promise)]
    (reify client/Client
      (setup! [this test node]
        (let [conn (c/await-client (c/connect node) test)]
          (when (deliver initialized? true)
            (c/sql! conn "create table sets (
                         id        integer primary key,
                         elements  string
                         ) with (number_of_replicas = \"0-all\")"))
          (client conn)))

      (invoke! [this test op]
        (let [[k v] (:value op)]
          (timeout 500 (assoc op :type :info, :error :timeout)
            (c/with-errors op
              (case (:f op)
                :read (->> (c/sql! conn "select elements
                                        from sets where id = ?" k)
                           :rows
                           first
                           :elements
                           json/parse-string
                           set
                           (independent/tuple k)
                           (assoc op :type :ok, :value))

                :add (if-let [cur (-> conn
                                      (c/sql! "select elements, _version
                                              from sets where id = ?"
                                              k)
                                      :rows
                                      first)]
                       (let [els' (-> (:elements cur)
                                      json/parse-string
                                      (conj v)
                                      json/generate-string)
                             _ (assert (number? (:_version cur)))
                             res (c/sql! conn "update sets set elements = ?
                                              where id = ? and _version = ?"
                                         els' k (:_version cur))]
                         (case (:row-count res)
                           0 (assoc op :type :fail)
                           1 (assoc op :type :ok)
                           2 (assoc op :type :info
                                    :error (str "Updated " (:row-count res)
                                                " rows!?"))))
                       (let [els' (json/generate-string [v])]
                         (c/sql! conn "insert into sets (id, elements)
                                      values (?, ?)"
                                 k els')
                         (assoc op :type :ok))))))))

      (teardown! [this test]
        (.close conn))))))

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
           {:name    "crate lost-updates"
            :os      debian/os
            :db      (c/db)
            :client  (client)
            :checker (checker/compose
                       {:set  (independent/checker checker/set)
                        :perf (checker/perf)})
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
