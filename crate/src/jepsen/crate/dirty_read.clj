(ns jepsen.crate.dirty-read
  "Searches for dirty reads."
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
            (c/sql! conn "create table dirty_read (
                           id integer primary key,
                         ) with (number_of_replicas = \"0-all\")"))
          (client conn)))

      (invoke! [this test op]
        (timeout 500 (assoc op :type :info, :error :timeout)
                 (c/with-errors op
                   (case (:f op)
                     ; Read a specific ID
                     :read (let [v (->> (c/sql! conn "select id from dirty_read
                                                     where id = ?"
                                                (:value op))
                                        :rows
                                        first
                                        :id)]
                             (assoc op :type (if v :ok :fail)))

                     ; Perform a full read of all IDs
                     :strong-read (->> (c/sql! conn
                                               "select id from sets")
                                       :rows
                                       (map :id)
                                       (into (sorted-set))
                                       (assoc op :type :ok, :value))

                     ; Add an ID
                     :write (do (c/sql! "insert into dirty_read (id) values (?)"
                                        (:value op))
                                (assoc op :type :ok))))))

      (teardown! [this test]
        (.close conn))))))

(defn checker
  "Verifies that we never read an element from a transaction which did not
  commmit (and hence was not visible in a final strong read).

  Also verifies that every successful write is present in the strong read set."
  []
  (reify checker/Checker
    (check [checker test model history opts]
      (let [ok    (filter op/ok? history)
            writes (->> ok
                        (filter #(= :write (:f %)))
                        (map :value)
                        (into (sorted-set)))
            reads (->> ok
                       (filter #(= :read (:f %)))
                       (map :value)
                       (into (sorted-set)))
            strong-read-sets (->> ok
                                  (filter #(= :strong-read (:f %)))
                                  (map :value))
            strong-reads (reduce set/union strong-read-sets)
            unseen       (set/difference strong-reads reads)
            dirty        (set/difference reads strong-reads)
            lost         (set/difference writes strong-reads)]
        ; We expect one strong read per node
        (info :strong-read-sets (count strong-read-sets))
        (info :concurrency (:concurrency test))
        (assert (= (count strong-read-sets) (:concurrency test)))
        ; All strong reads had darn well better be equal
        (assert (apply = (map count (cons strong-reads strong-read-sets))))

        {:valid?            (and (empty? dirty) (empty? lost))
         :read-count        (count reads)
         :strong-read-count (count strong-reads)
         :unseen-count      (count unseen)
         :dirty-count       (count dirty)
         :dirty             dirty
         :lost-count        (count lost)
         :lost              lost}))))

(defn sr  [_ _] {:type :invoke, :f :strong-read, :value nil})

(defn rw-gen
  "While one process writes to a node, we want another process to see that the
  in-flight write is visible, in the instant before the node crashes."
  []
  (let [; What did we write last?
        write (atom -1)
        ; A vector of in-flight writes on each node.
        in-flight (atom nil)]
    (reify gen/Generator
      (op [_ test process]
        ; lazy init of in-flight state
        (when-not @in-flight
          (compare-and-set! in-flight
                            nil
                            (vec (repeat (count (:nodes test)) 0))))

        (let [; thread index
              t (gen/process->thread test process)
              ; node index
              n (mod process (count (:nodes test)))]
          (if (= t n)
            ; The first n processes perform writes
            (let [v (swap! write inc)]
              ; Record the in-progress write
              (swap! in-flight assoc n v)
              {:type :invoke, :f :write, :value v})

            ; Remaining processes try to read the most recent write
            {:type :invoke, :f :read, :value (nth @in-flight n)}))))))

(defn test
  [opts]
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
          :generator (gen/phases
                       (->> (rw-gen)
                            (gen/stagger 1/100)
                            (gen/nemesis
                              (gen/seq (cycle [(gen/sleep 40)
                                               {:type :info, :f :start}
                                               (gen/sleep 120)
                                               {:type :info, :f :stop}])))
                            (time-limit 200))
                       (gen/nemesis (gen/once {:type :info :f :stop}))
                       (gen/log "Waiting for quiescence")
                       (gen/sleep 30)
                       (gen/clients (gen/each (gen/once sr))))}
         opts))
