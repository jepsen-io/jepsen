(ns jepsen.rethinkdb.document-cas
  "Compare-and-set against a single document."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [util      :as util :refer [meh timeout]]
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

(defn spy [x]
  (info (with-out-str (pprint x)))
  x)

(defrecord Client [db tbl primary write_acks read_mode]
  client/Client
  (setup! [this test node]
    (info node "Connecting CAS Client...")
    (info node (str `(connect :host ~(name node) :port 28015)))
    (let [conn (connect :host (name node) :port 28015)]
      (info node "Connecting CAS Client DONE!")
      (when (= node (jepsen/primary test))
        (info node "Creating table...")
        (r/run (r/db-create db) conn)
        (r/run (r/table-create (r/db db) tbl {:replicas 5}) conn)
;        (r/run (r/insert (r/table (r/db db) tbl) {:id id :val nil}) conn)
        (pr (r/run
              (r/update
                (r/table (r/db "rethinkdb") "table_config")
                {:write_acks write_acks
                 :shards [{:primary_replica primary
                           :replicas ["n1" "n2" "n3" "n4" "n5"]}]})
              conn))
        (r/run
          (rethinkdb.query-builder/term :WAIT [(r/table (r/db db) tbl)] {})
          conn)
        (info node "Creating table DONE!"))

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
                 (info res)
                 (assoc op :type (if (and (= (:errors res) 0)
                                          (= (:replaced res) 1))
                                   :ok
                                   :fail)))))))

  (teardown! [this test]
    (r/run (r/db-drop db) (:conn this))
    (close (:conn this))))

(defn client
  "A client which implements a register on top of an entire document."
  [write_acks read_mode]
  (Client. "jepsen" "cas" "n5" write_acks read_mode))

; Generators
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn safe-test
  "Document-level compare and set with safe settings."
  [version]
  ;; This is the only safe read/write mode.  Changing either of
  ;; these (or turning on soft durability) may produce a
  ;; non-linearizable history.
  (test- "document write-majority read-majority"
         {:version version
          :client (client "majority" "majority")
          :generator (std-gen (independent/sequential-generator
                                (range)
                                (fn [k]
                                  (gen/mix [r w cas cas]))))
          :checker (checker/compose
                     {:linear (independent/checker checker/linearizable)
                      :perf   (checker/perf)})}))
                                  
