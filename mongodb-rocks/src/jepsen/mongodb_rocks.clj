(ns jepsen.mongodb-rocks
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.mongodb.core :as mongo]
            [jepsen [client :as client]
                    [db :as db]
                    [tests :as tests]
                    [control :as c]
                    [checker :as checker]
                    [generator :as gen]
                    [util :refer [timeout]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [monger [core :as m]
                    [collection :as mc]
                    [result :as mr]
                    [query :as mq]
                    [command]
                    [operators :refer :all]
                    [conversion :refer [from-db-object]]]
            [clj-time [core :as time]
                      [format :as time.format]
                      [coerce :as time.coerce]])
  (:import (com.mongodb DB
                        WriteConcern
                        ReadPreference)))

(defn install!
  "Download and install rocksdb packages."
  [node version]
  (c/su
    (c/cd "/tmp"
          (let [url (str "https://s3.amazonaws.com/parse-mongodb-builds/debs/"
                          "mongodb-org-server_" version "_amd64.deb")
                file (cu/wget! url)]
            (info node "installing" file)
            (c/exec :dpkg :-i, :--force-confask :--force-confnew file)
            (mongo/stop! node)))))

(defn configure!
  "Deploy configuration files to the node."
  [node engine]
  (c/exec :echo (-> "mongod.conf" io/resource slurp
                    (str/replace #"%ENGINE%" engine))
          :> "/etc/mongod.conf"))

(defn db
  "RocksDB variant of MongoDB."
  [version engine]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)
        (configure! engine)
        (mongo/start!)
        (mongo/join! test)))

    (teardown! [_ test node]
      (mongo/wipe! node))

    db/LogFiles
    (log-files [_ _ _]
      ["/var/log/mongodb/mongod.log"])))

(def printable-ascii (->> (concat (range 48 68)
                                  (range 66 92)
                                  (range 97 123))
                          (map char)
                          char-array))

(defn rand-str
  "Random ascii string of n characters"
  [n]
  (let [s (StringBuilder. n)]
    (dotimes [i n]
      (.append s ^char
               (->> printable-ascii
                    alength
                    rand-int
                    (aget printable-ascii))))
    (.toString s)))

(def payload (rand-str (* 100 1024)))

(defrecord Client [db-name coll write-concern conn db]
  client/Client

  (setup! [this test node]
    (let [conn (mongo/cluster-client test)
          db   (m/get-db conn db-name)]
      ; Ensure index exists
      (timeout (* 20 1000)
               (throw (ex-info "Timed out trying to ensure index" {:node node}))
               (loop []
                 (or (try
                       (mc/ensure-index db coll (array-map :time 1))
                       true
                       (catch com.mongodb.MongoServerSelectionException e
                         false))
                     (recur))))
      (assoc this :conn conn, :db db)))

  (invoke! [this test op]
    (mongo/with-errors op #{}
      (case (:f op)
        :write (let [res (mongo/parse-result
                           (mc/insert db coll
                                      {:_id (:value op)
                                       :time (System/currentTimeMillis)
                                       :payload payload}
                                      write-concern))]
                 (assoc op :type :ok))
        :delete (let [res (mc/find-and-modify db coll
                                              {}
                                              {}
                                              {:sort {:time 1}
                                               :remove true})]
                  (if-let [id (:_id res)]
                    (assoc op :type :ok, :value id)
                    (assoc op :type :fail))))))

  (teardown! [_ test]
    (m/disconnect conn)))

(defn client
  "A client for the logger"
  []
  (Client. "jepsen"
           "logger"
           WriteConcern/ACKNOWLEDGED
           nil
           nil))

(defn writes
  []
  (reify gen/Generator
    (op [_ test process]
      {:type :invoke,
       :f :write,
       :value (-> (time/now)
                  (time.coerce/to-long)
                  (/ 1000)
                  long
                  (str "-oempa_" (rand-int Integer/MAX_VALUE)))})))

(defn deletes
  []
  {:type :invoke
   :f :delete
   :value nil})

(defn generator
  []
  (gen/mix [(writes) (writes) (deletes)]))

(defn logger-perf-test
  [version engine]
  (assoc tests/noop-test
         :name    (str "mongodb queue " version " " engine)
         :os      debian/os
         :db      (db version engine)
         :checker (checker/compose {:latency (checker/perf)})
         :client  (client)
         :concurrency 100
         :generator (->> (generator)
                         (gen/clients)
;                         (gen/delay 1)
                         (gen/time-limit 1000))))
