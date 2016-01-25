(ns jepsen.rethinkdb.document-cas
  "Compare-and-set against a single document."
  (:refer-clojure :exclude [run!])
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [util      :as util :refer [meh timeout retry]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]
                    [independent :as independent]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [jepsen.rethinkdb :refer :all]
            [rethinkdb.core :refer [connect close]]
            [rethinkdb.query :as r]
            [rethinkdb.query-builder :refer [term]])
  (:import (clojure.lang ExceptionInfo)))

(defn set-write-acks!
  "Updates the write-acks mode for a cluster. Spins until successful."
  [conn test write-acks]
  (retry 5
         (run!
           (r/update
             (r/table (r/db "rethinkdb") "table_config")
             {:write_acks write-acks
              :shards [{:primary_replica (jepsen/primary test)
                        :replicas (map name (:nodes test))}]})
           conn)))

(defn set-heartbeat!
  "Set the heartbeat on a cluster to dt seconds"
  [conn dt]
  (-> (r/table (r/db "rethinkdb") "cluster_config")
      (r/get "heartbeat")
      (r/update {:heartbeat_timeout_secs dt})
      (run! conn)))

; r.db('rethinkdb').table('cluster_config').get("heartbeat").update({heartbeat_timeout_secs: 2})

(defrecord Client [table-created? db tbl write-acks read_mode]
  client/Client
  (setup! [this test node]
    (let [conn (conn node)]
      (info node "Connected")
      ; Everyone's gotta block until we've made the table.
      (locking table-created?
        (when-not (realized? table-created?)
          (info node "Creating table...")
          (run! (r/db-create db) conn)
          (run! (r/table-create (r/db db) tbl {:replicas 5}) conn)
          (set-write-acks! conn test write-acks)
          (set-heartbeat! conn 2)
          (wait-table conn db tbl)
          (info node "Table created")
          (deliver table-created? true)))

      (assoc this :conn conn :node node)))

  (invoke! [this test op]
    (with-errors op #{:read}
      (let [id    (key (:value op))
            value (val (:value op))
            row (r/get (rethinkdb.query-builder/term
                         :TABLE
                         [(r/db db) tbl]
                         {:read_mode read_mode})
                       id)]
        (case (:f op)
          :read (assoc op
                       :type  :ok
                       :value (independent/tuple
                                id
                                (r/run (term :DEFAULT
                                             [(r/get-field row "val") nil])
                                       (:conn this))))
          :write (do (run! (r/insert (r/table (r/db db) tbl)
                                     {:id id, :val value}
                                     {"conflict" "update"})
                           (:conn this))
                     (assoc op :type :ok))
          :cas (let [[value value'] value
                     res (r/run
                           (r/update
                             row
                             (r/fn [row]
                               (r/branch
                                 (r/eq (r/get-field row "val") value)
                                 {:val value'}
                                 (r/error "abort"))))
                           (:conn this))]
                 (assoc op :type (if (and (= (:errors res) 0)
                                          (= (:replaced res) 1))
                                   :ok
                                   :fail)))))))

  (teardown! [this test]
    (close (:conn this))))

(defn client
  "A client which implements a register on top of an entire document."
  [write_acks read_mode]
  (Client. (promise) "jepsen" "cas" write_acks read_mode))

; Generators
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn start-stop
  "Infinite seq of start and stop generators separated by 5 seconds of sleeps."
  []
  (cycle [(gen/sleep 5)
          {:type :info :f :start}
          (gen/sleep 5)
          {:type :info :f :stop}]))

(defn cas-test
  "Document-level compare and set with the given read and write mode."
  [version write-acks read-mode]
  (test- (str "document write-" write-acks " read-" read-mode)
         {:version version
          :client (client write-acks read-mode)
          :concurrency 10
          :generator (->> (independent/sequential-generator
                            (range)
                            (fn [k]
                              (->> (gen/reserve 5 (gen/mix [w cas])
                                                r)
                                   (gen/delay 1)
                                   (gen/limit 60))))
                          (gen/nemesis (gen/seq (start-stop)))
                          (gen/time-limit 500))
          :nemesis (nemesis/partition-random-halves)
          :checker (checker/compose
                     {:linear (independent/checker checker/linearizable)
                      :perf   (checker/perf)})}))

(defn cas-reconfigure-test
  "Document-level compare and set with majority reads and writes *and* a
  combination of topology changes and network partitions. Performs only writes
  and cas ops to prove that data loss isn't just due to stale reads."
  [version]
  (let [t (cas-test version "majority" "majority")]
    (assoc t
           :name      "rethinkdb document reconfigure"
           :generator (->> (independent/sequential-generator
                             (range)
                             (fn [k]
                               (->> (gen/reserve 5 (gen/mix [w cas])
                                                 r)
                                    (gen/delay 1)
                                    (gen/limit 100))))
                         ;(gen/nemesis
                         ;  (gen/phases
                         ;    (gen/await #(deref (:table-created? (:client t))))
                         ;    (gen/seq (cycle [(gen/sleep 0)
                         ;                     {:type :info :f :reconfigure}]))))
                           (gen/nemesis
                             (gen/phases
                               (gen/await
                                 (fn []
                                   (info "Nemesis waiting")
                                   (deref (:table-created? (:client t)))
                                   (info "Nemesis ready to go")))
                               (->> (cycle [{:type :info, :f :start}
                                            {:type :info, :f :stop}])
                                    (interpose {:type :info, :f :reconfigure})
                                    (gen/seq))))
                           (gen/time-limit 1000))
;         :nemesis (aggressive-reconfigure-nemesis "jepsen" "cas"))))
           :nemesis  (nemesis/compose
                       {#{:reconfigure} (reconfigure-nemesis "jepsen" "cas")
                        #{:start :stop} (nemesis/partition-random-halves)}))))
