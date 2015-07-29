(ns rdb.document-cas
  "Compare-and-set against a single document."
  (:require [clojure [pprint :refer :all]
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
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [rdb.core :refer :all]
            [rethinkdb.core :refer [connect close]]
            [rethinkdb.query :as r]
            [knossos.core :as knossos]
            [cheshire.core :as json])
  (:import (clojure.lang ExceptionInfo)))

(defrecord Client [db tbl id primary write_acks read_mode]
  client/Client
  (setup! [this test node]
    (info node "Connecting CAS Client...")
    (info node (str `(connect :host ~(name node) :port 28015)))
    (let [conn (connect :host (name node) :port 28015)]
      (info node "Connecting CAS Client DONE!")
      (if (= node :n1)
        (do
          (info node "Creating table...")
          (r/run (r/db-create db) conn)
          (r/run (r/table-create (r/db db) tbl {:replicas 5}) conn)
          (r/run (r/insert (r/table (r/db db) tbl) {:id id :val nil}) conn)
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
        (info node "Not creating table."))
      (assoc this :conn conn :node node)))

  (invoke! [this test op]
    (let [fail (if (= :read (:f op)) :fail :info)
          row (r/get
               (rethinkdb.query-builder/term
                :TABLE
                [(r/db db) tbl]
                {:read_mode read_mode})
               id)]
      (with-errors op #{:read}
       (case (:f op)
         :read (assoc
                op
                :type :ok
                :value (r/run (r/get-field row "val") (:conn this)))
         :write (do (r/run
                      (r/update row {:val (:value op)})
                      (:conn this))
                    (assoc op :type :ok))
         :cas (let [[value value'] (:value op)
                    res (r/run
                          (r/update
                           row
                           (r/fn [row]
                             (r/branch
                              (r/eq (r/get-field row "val") value)
                              {:val value'}
                              (r/error "abort"))))
                          (:conn this))]
                (assoc op :type (if (= (:errors res) 0) :ok :fail)))))))

  (teardown! [this test]
    (r/run (r/db-drop db) (:conn this))
    (close (:conn this))))

(defn client
  "A client which implements a register on top of an entire document."
  [write_acks read_mode]
  (Client. "jepsen" "cas" 0 "n5" write_acks read_mode))

; Generators
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn safe-test
  "Document-level compare and set with safe settings."
  []
  ;; This is the only safe read/write mode.  Changing either of
  ;; these (or turning on soft durability) may produce a
  ;; non-linearizable history.
  (test- "document {:write_acks 'majority' :read_mode 'majority'}"
         {:client (client "majority" "majority")
          :generator (std-gen (gen/mix [r w cas cas]))}))
