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
            [jepsen.crate.core    :as c]
            [clojure.java.jdbc    :as j]
            [clojure.string       :as str]
            [clojure.set          :as set]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [knossos.op           :as op])
  (:import (org.elasticsearch.action NoShardAvailableActionException)
           (org.elasticsearch.client.transport
             TransportClient
             NoNodeAvailableException)))

(defrecord DirtyReadClient [tbl-created? conn limit]
  client/Client

  (setup! [this test])

  (open! [this test node]
    (let [conn (c/jdbc-client node)]
      (info node "Connected")
      ;; Everyone's gotta block until we've made the table.
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (c/with-conn [c conn]
            (j/execute! c ["drop table if exists dirty_read"])
            (info node "Creating table dirty_read")
            (j/execute! c
                         ["create table dirty_read (
                          id     integer primary key)"])
            (j/execute! c
                         ["alter table dirty_read
                          set (number_of_replicas = \"0-all\")"]))))

      (assoc this :conn conn)))

  (invoke! [this test op]
    (c/with-exception->op op
      (c/with-conn [c conn]
        (c/with-timeout
          (c/with-txn [c c]
            (assoc op :type :info, :error :timeout)
            (c/with-errors op
              (case (:f op)
                ; Read a specific ID
                :read (let [v (->> (c/query c  ["select id from dirty_read
                                                    where id = ?"
                                                    (:value op)])
                                   first
                                   :id)]
                        (assoc op :type (if v :ok :fail)))

                ; Refresh table
                :refresh (do (j/execute! c ["refresh table dirty_read"] 
                                         {:timeout 60000})
                             (assoc op :type :ok))

                ; Perform a full read of all IDs
                :strong-read
                (do (->> (c/query c
                                  ["select id from dirty_read LIMIT ?"
                                   (+ 100 @limit)]) ; who knows
                         (map :id)
                         (into (sorted-set))
                         (assoc op :type :ok, :value)))

                ; Add an ID
                :write (do (swap! limit inc)
                           (j/execute! c
                                       ["insert into dirty_read (id) values (?)"
                                        (:value op)]
                                       {:timeout c/timeout-delay})
                           (assoc op :type :ok)))))))))

        (close! [this test])

        (teardown! [this test]
                   ))

(defn es-client
  "Elasticsearch based client. Wraps an underlying Crate client for some ops.
  Options:

  :es-ops     Set of operation :f's to put through elasticsearch (instead of
  crate)"
  ([opts] (es-client (:es-ops opts) (DirtyReadClient. (atom false) nil (atom 0)) nil))
  ([es-ops crate ^TransportClient es]
   (assert (set? es-ops))
   (reify client/Client
     (open! [this test node]
       (let [crate (client/open! crate test node)
             es    (c/es-connect node)]
         (es-client es-ops crate es)))

     (setup! [this test])

     (invoke! [this test op]
       (if-not (es-ops (:f op))
         (client/invoke! crate test op)

         (timeout 10000 (assoc op :type :info, :error :timeout)
                  (case (:f op)
                    :strong-read
                    (->> (c/es-search es)
                         (map (comp :id :source))
                         (into (sorted-set))
                         (assoc op :type :ok, :value))

                    :read
                    (let [v (-> (c/es-get es "dirty_read" "default" (:value op))
                                :source
                                :id)]
                      (assoc op :type (if v :ok :fail)))

                    :write
                    (do (c/es-index! es "dirty_read" "default"
                                     {:id (:value op)})
                        (assoc op :type :ok))))))

     (close! [this test])

     (teardown! [this test]
       (client/teardown! crate test)
       (.close es)))))

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
            on-all       (reduce set/intersection strong-read-sets)
            on-some      (reduce set/union strong-read-sets)
            not-on-all   (set/difference on-some  on-all)
            unchecked    (set/difference on-some  reads)
            dirty        (set/difference reads    on-some)
            lost         (set/difference writes   on-some)
            some-lost    (set/difference writes   on-all)
            nodes-agree? (= on-all on-some)]
        ; We expect one strong read per node
        (info :strong-read-sets (count strong-read-sets))
        (info :concurrency (:concurrency test))

        ; Everyone should have read something
        (assert (= (count strong-read-sets) (:concurrency test)))

        {:valid?                         (and nodes-agree?
                                              (empty? dirty)
                                              (empty? lost))
         :nodes-agree?                   nodes-agree?
         :read-count                     (count reads)
         :on-all-count                   (count on-all)
         :on-some-count                  (count on-some)
         :unchecked-count                (count unchecked)
         :not-on-all-count               (count not-on-all)
         :not-on-all                     not-on-all
         :dirty-count                    (count dirty)
         :dirty                          dirty
         :lost-count                     (count lost)
         :lost                           lost
         :some-lost-count                (count some-lost)
         :some-lost                      some-lost}))))

(defn sr  [_ _] {:type :invoke, :f :strong-read, :value nil})

(defn rw-gen
  "While one process writes to a node, we want another process to see that the
  in-flight write is visible, in the instant before the node crashes. w
  controls the number of writers."
  [w]
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
          (if (< t w)
            ; The first w processes perform writes
            (let [v (swap! write inc)]
              ; Record the in-progress write
              (swap! in-flight assoc n v)
              {:type :invoke, :f :write, :value v})

            ; Remaining processes try to read the most recent write
            {:type :invoke, :f :read, :value (nth @in-flight n)}))))))

(defn test
  "Options:

  :concurrency  - Number of concurrent clients
  :es-ops       - Set of operations to perform using an ES client directly
  :time-limit   - Time, in seconds, to run the main body of the test."
  [opts]
  (merge tests/noop-test
         {:name    (let [o (:es-ops opts)]
                     (str "dirty-read "
                          (str/join " " [(str "r="  (if (:read o)  "e" "c"))
                                         (str "w="  (if (:write o) "e" "c"))
                                         (str "sr=" (if (:strong-read o)
                                                      "e" "c"))])))
          :os      debian/os
          :db      (c/db (:tarball opts))
          :client  (es-client opts)
          :checker (checker/compose
                     {:dirty-read (checker)
                      :perf       (checker/perf)})
          :concurrency (:concurrency opts)
          :nemesis (nemesis/partition-random-halves)
          :generator (gen/phases
                       (->> (rw-gen (/ (:concurrency opts) 3))
                            (gen/stagger 1/10)
                            (gen/nemesis ;nil)
                                         (gen/seq (cycle [(gen/sleep 10)
                                                          {:type :info, :f :start}
                                                          (gen/sleep 20)
                                                          {:type :info, :f :stop}])))
                            (gen/time-limit (:time-limit opts)))
                       (gen/nemesis (gen/once {:type :info :f :stop}))
                       (gen/clients (gen/each
                                      (gen/once {:type :invoke, :f :refresh})))
                       (gen/log "Waiting for quiescence")
                       (gen/sleep 10)
                       (gen/clients (gen/each (gen/once sr))))}
         opts))
