(ns jepsen.mongodb.document-cas
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
                    [independent :as independent]
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
            [knossos.model :as model]
            [cheshire.core :as json]
            [jepsen.mongodb.core :refer :all]
            [jepsen.mongodb.mongo :as m])
  (:import (clojure.lang ExceptionInfo)))

(defrecord Client [db-name coll-name write-concern client coll]
  client/Client
  (setup! [this test node]
    (let [client (m/cluster-client test)
          coll   (-> client
                     (m/db db-name)
                     (m/collection coll-name)
                     (m/with-write-concern write-concern))]

      (assoc this :client client, :coll coll)))

  (invoke! [this test op]
    ; Reads are idempotent; we can treat their failure as an info.
    (with-errors op #{:read}
      (let [id    (key (:value op))
            value (val (:value op))]
        (case (:f op)
          :read (let [res (m/find-one coll id)]
                  (assoc op
                         :type  :ok
                         :value (independent/tuple id (:value res))))

          :write (let [res (m/upsert! coll {:_id id, :value value})]
                   ; Note that modified-count could be zero, depending on the
                   ; storage engine, if you perform a write the same as the
                   ; current value.
                   (assert (< (:matched-count res) 2))
                   (assoc op :type :ok))

          :cas   (let [[value value'] value
                       res (m/cas! coll
                                   {:_id id, :value value}
                                   {:_id id, :value value'})]
                   ; Check how many documents we actually modified.
                   (condp = (:matched-count res)
                     0 (assoc op :type :fail)
                     1 (assoc op :type :ok)
                     true (assoc op :type :info
                                 :error (str "CAS: matched too many docs! "
                                             res))))))))
  (teardown! [_ test]
    (.close ^java.io.Closeable client)))

(defn client
  "A client which implements a register on top of an entire document."
  [write-concern]
  (Client. "jepsen"
           "cas"
           write-concern
           nil
           nil))

; Generators
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn majority-test
  "Document-level compare and set with WriteConcern MAJORITY"
  [opts]
  (test- "document cas majority"
         (merge
           {:client       (client :majority)
            :concurrency  10
            :generator    (std-gen
                            (independent/sequential-generator
                              (range)
                              (fn [k]
                                (->> (gen/mix [w cas cas])
                                     (gen/reserve 5 r)
                                     (gen/stagger 1)
                                     (gen/time-limit 45)))))
            :model        (model/cas-register)
            :checker      (checker/compose
                            {:linear (independent/checker checker/linearizable)
                             :perf   (checker/perf)})}
           opts)))

(defn no-read-majority-test
  "Document-level compare and set with MAJORITY, excluding reads because mongo
  doesn't have linearizable reads."
  [opts]
  (test- "document cas no-read majority"
         (merge {:client (client :majority)
                 :generator (std-gen (gen/mix [w cas cas]))}
                opts)))
