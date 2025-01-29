(ns jepsen.mongodb-smartos.document-cas
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
            [knossos.core :as knossos]
            [cheshire.core :as json]
            [jepsen.mongodb-smartos.core :refer :all]
            [monger.core :as mongo]
            [monger.collection :as mc]
            [monger.result :as mr]
            [monger.query :as mq]
            [monger.command]
            [monger.operators :refer :all]
            [monger.conversion :refer [from-db-object]])
  (:import (clojure.lang ExceptionInfo)
           (org.bson BasicBSONObject
                     BasicBSONDecoder)
           (org.bson.types ObjectId)
           (com.mongodb DB
                        WriteConcern
                        ReadPreference)))

(defrecord Client [db-name coll id write-concern conn db]
  client/Client
  (setup! [this test node]
    (let [conn (cluster-client test)
          db   (mongo/get-db conn db-name)]
      ; Create document
      (upsert db coll {:_id id, :value nil} write-concern)

      (assoc this :conn conn, :db db)))

  (invoke! [this test op]
    ; Reads are idempotent; we can treat their failure as an info.
    (with-errors op #{:read}
      (case (:f op)
        ; :read (let [res (mc/find-map-by-id db coll id)]
        :read (let [res (->> (mq/with-collection db coll
                               (mq/find {:_id id})
                               (mq/fields [:_id :value])
                               (mq/read-preference (ReadPreference/primary)))
                             first)]
                (assoc op :type :ok, :value (:value res)))

        :write (let [res (parse-result
                           (mc/update-by-id db coll id
                                            {:_id id, :value (:value op)}
                                            {:write-concern write-concern}))]
                 (assert (= 1 (.getN res)))
                 (assoc op :type :ok))

        :cas   (let [[value value'] (:value op)
                     res (parse-result
                           (mc/update db coll
                                      {:_id id, :value value}
                                      {:_id id, :value value'}
                                      {:write-concern write-concern}))]
                 ; Check how many documents we actually modified...
                 (condp = (.getN res)
                   0 (assoc op :type :fail)
                   1 (assoc op :type :ok)
                   2 (throw (ex-info "CAS unexpected number of modified docs"
                                     {:n (.getN res)
                                      :res res})))))))

  (teardown! [_ test]
    (mongo/disconnect conn)))

(defn client
  "A client which implements a register on top of an entire document."
  [write-concern]
  (Client. "jepsen"
           "jepsen"
           (ObjectId.)
           write-concern
           nil
           nil))

; Generators
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn majority-test
  "Document-level compare and set with WriteConcern MAJORITY"
  []
  (test- "document cas majority"
         {:client (client WriteConcern/MAJORITY)
          :generator (std-gen (gen/mix [r w cas cas]))}))

(defn no-read-majority-test
  "Document-level compare and set with MAJORITY, excluding reads because mongo
  doesn't have linearizable reads."
  []
  (test- "document cas no-read majority"
         {:client (client WriteConcern/MAJORITY)
          :generator (std-gen (gen/mix [w cas cas]))}))
