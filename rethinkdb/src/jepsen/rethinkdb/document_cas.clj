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
            [rethinkdb.query-builder :refer [term]]
            [knossos.core :as knossos]
            [cheshire.core :as json])
  (:import (clojure.lang ExceptionInfo)))

(defn run!
  "Like rethinkdb.query/run, but asserts that there were no errors."
  [query conn]
  (let [result (r/run query conn)]
    (when (contains? result :errors)
      (assert (zero? (:errors result)) (:first_error result)))
    result))

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

(defn set-heartbeat
  "Set the heartbeat on a cluster to dt seconds"
  [conn dt]
; r.db('rethinkdb').table('cluster_config').get("heartbeat").update({heartbeat_timeout_secs: 2})
)

(defn wait-table
  "Wait for all replicas for a table to be ready"
  [conn db tbl]
  (run! (term :WAIT [(r/table (r/db db) tbl)] {}) conn))

(defrecord Client [db tbl-created? tbl primary write-acks read_mode]
  client/Client
  (setup! [this test node]
    (info node "Connecting...")
    (let [conn (connect :host (name node) :port 28015)]
      (info node "Connected")
      ; Everyone's gotta block until we've made the table.
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (info node "Creating table...")
          (run! (r/db-create db) conn)
          (run! (r/table-create (r/db db) tbl {:replicas 5}) conn)
          (set-write-acks! conn test write-acks)
          (wait-table conn db tbl)
          (info node "Table created")))

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
          :write (do (r/run (r/insert (r/table (r/db db) tbl)
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
    (meh (r/run (r/db-drop db) (:conn this)))
    (close (:conn this))))

(defn client
  "A client which implements a register on top of an entire document."
  [write_acks read_mode]
  (Client. "jepsen" (atom false) "cas" "n5" write_acks read_mode))

; Generators
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn cas-test
  "Document-level compare and set with the given read and write mode."
  [version write-acks read-mode]
  (test- (str "document write-" write-acks " read-" read-mode)
         {:version version
          :client (client write-acks read-mode)
          :concurrency 10
          :generator (std-gen (independent/sequential-generator
                                (range)
                                (fn [k]
                                  ; Do a mix of reads, writes, and CAS ops in
                                  ; quick succession
                                  (->> (gen/mix [r w])
                                       (gen/stagger 1/2)
                                       (gen/limit 1000)))))
          :checker (checker/compose
                     {:linear (independent/checker checker/linearizable)
                      :perf   (checker/perf)})}))
