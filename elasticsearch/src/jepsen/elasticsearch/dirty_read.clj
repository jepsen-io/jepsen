(ns jepsen.elasticsearch.dirty-read
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
            [jepsen.checker.timeline    :as timeline]
            [jepsen.elasticsearch.core  :as e]
            [clojure.string       :as str]
            [clojure.set          :as set]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [knossos.op           :as op])
  (:import (org.elasticsearch.action NoShardAvailableActionException)
           (org.elasticsearch.client.transport
             TransportClient
             NoNodeAvailableException)))

(def index-name "dirty_read")

(defn client
  "Elasticsearch based client."
  ([] (client nil))
  ([^TransportClient es]
   (reify client/Client
     (setup! [this test node]
       (let [es (e/es-connect node)]
         (try
           (-> es
               (.admin)
               (.indices)
               (.prepareCreate index-name)
               (.execute)
               (.actionGet)
               (.isAcknowledged)
               (info :acked))
           (catch org.elasticsearch.indices.IndexAlreadyExistsException e))
         (client es)))

     (invoke! [this test op]
       (timeout (case (:f op)
                  :refresh      120000
                  :strong-read  60000
                  :write        10000
                  100)
                (assoc op :type :info, :error :timeout)
                (try
                  (case (:f op)
                    :refresh
                    (util/with-retry []
                      (let [r (-> es
                                  (.admin)
                                  (.indices)
                                  (.prepareRefresh
                                    (into-array String [index-name]))
                                  (.get))]
                        (if (= (.getTotalShards r)
                               (.getSuccessfulShards r))
                          (assoc op :type :ok)
                          (do (info "Refresh failed; not all shards successful."
                                    :total (.getTotalShards r)
                                    :success (.getSuccessfulShards r)
                                    :failed (.getFailedShards r)
                                    (->> r
                                         .getShardFailures
                                         (map (fn [s]
                                                {:shard-id (.shardId s)
                                                 :reason (.reason s)
                                                 :status (.status s)
                                                 :cause  (.getCause s)}))))
                              (retry)))))

                    :strong-read
                    (->> (e/es-search es)
                         (map (comp :id :source))
                         (into (sorted-set))
                         (assoc op :type :ok, :value))

                    :read
                    (let [v (-> (e/es-get es index-name "default" (:value op))
                                :source
                                :id)]
                      (assoc op :type (if v :ok :fail)))

                    :write
                    (do (e/es-index! es index-name "default"
                                     {:id (:value op)})
                        (assoc op :type :ok)))
                  (catch NoShardAvailableActionException e
                    (assoc op :type :info, :error :no-shard-available)))))

     (teardown! [this test]
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

  :tarball      - ES tarball URL
  :concurrency  - Number of concurrent clients
  :time-limit   - Time, in seconds, to run the main body of the test."
  [opts]
  (merge tests/noop-test
         {:name    "elasticsearch dirty read"
          :os      debian/os
          :db      (e/db (:tarball opts))
          :client  (client)
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
