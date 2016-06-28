(ns jepsen.crate
  (:require [jepsen [core         :as jepsen]
                    [db           :as db]
                    [control      :as c :refer [|]]
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
            [jepsen.control.util  :as cu]
            [jepsen.control.net   :as cnet]
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

;; Client
(defn connect
  "Opens a connection to a node."
  [node]
  (CrateClient. (into-array String [(name node)])))

(defn parse
  "Parse an SQLResponse into a sequence of maps."
  [^SQLResponse res]
  {:row-count (when (.hasRowCount res) (.rowCount res))
   :duration  (.duration res)
   :rows      (if (zero? (alength (.cols res)))
                (repeat (count (.rows res)) {})
                (let [columns (map keyword (.cols res))
                      basis   (apply create-struct columns)]
                  (map (fn structer [row]
                         (clojure.lang.PersistentStructMap/construct
                           basis (seq row)))
                       (.rows res))))})

(defn sql!
  "Execute an SQL statement with a client and return its parsed results."
  [^CrateClient client statement & args]
  (let [req (SQLRequest. statement (into-array Object args))]
    (-> client
        (.sql req)
        .actionGet
        parse)))

(defn await-client
  "Takes a client and waits for it to become ready"
  [client test]
  (timeout 120000
           (throw (RuntimeException. (str client " did not start up")))
           (with-retry []
             (sql! client "select * from sys.nodes")
             client
             (catch NoNodeAvailableException e
               (Thread/sleep 1000)
               (retry)))))

;; DB

(defn install!
  "Install crate."
  [node]
  (c/su
    (debian/install [:apt-transport-https])
    (debian/install-jdk8!)
    (c/cd "/tmp"
          (c/exec :wget "https://cdn.crate.io/downloads/apt/DEB-GPG-KEY-crate")
          (c/exec :apt-key :add "DEB-GPG-KEY-crate")
          (c/exec :rm "DEB-GPG-KEY-crate"))
    (debian/add-repo! "crate" "deb https://cdn.crate.io/downloads/apt/stable/ jessie main")
    (debian/install [:crate])
    (c/exec :update-rc.d :crate :disable)))

(defn majority
  "n/2+1"
  [n]
  (-> n (/ 2) inc Math/floor long))

(defn configure!
  "Set up config files."
  [node test]
  (c/su
    (c/exec :echo
            (-> "crate.yml"
                io/resource
                slurp
                (str/replace "$NAME" (name node))
                (str/replace "$N" (str (count (:nodes test))))
                (str/replace "$MAJORITY" (str (majority (count (:nodes test)))))
                (str/replace "$HOSTS"
                             (json/generate-string
                               (vals (c/on-nodes test (fn [_ _]
                                                        (cnet/local-ip)))))))
            :> "/etc/crate/crate.yml")))

(defn start!
  [node]
  (c/su
    (c/exec :service :crate :start)))

(defn db
  []
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install!)
        (configure! test)
        (start!)))

    (teardown! [_ test node]
      (cu/grepkill! "crate")
      (c/exec :rm :-rf (c/lit "/var/log/crate/*"))
      (c/exec :rm :-rf (c/lit "/var/lib/crate/*")))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/crate/crate.log"])))

(defn client
  ([] (client nil))
  ([conn]
   (let [initialized? (promise)]
    (reify client/Client
      (setup! [this test node]
        (let [conn (await-client (connect node) test)]
          (when (deliver initialized? true)
            (sql! conn "create table registers (
                          id     integer primary key,
                          value  integer
                       ) with (number_of_replicas = \"0-all\")"))
          (client conn)))

      (invoke! [this test op]
        (let [[k v] (:value op)]
          (timeout 500 (assoc op :type :fail)
            (try
              (case (:f op)
                :read (->> (sql! conn "select value, \"_version\"
                                 from registers where id = ?" k)
                           :rows
                           first
                           (independent/tuple k)
                           (assoc op :type :ok, :value))

                :write (let [res (sql! conn "insert into registers (id, value)
                                            values (?, ?)
                                            on duplicate key update
                                            value = VALUES(value)"
                                       k v)]
                         (assoc op :type :ok)))
              (catch SQLActionException e
                (cond
                  (and (= 5000 (.errorCode e))
                       (re-find #"blocked by: \[.+no master\];" (str e)))
                  (assoc op :type :fail, :error :no-master)

                  (and (= 5000 (.errorCode e))
                       (re-find #"rejected execution" (str e)))
                  (assoc op :type :info, :error :rejected-execution)

                  :else
                  (throw e)))))))

      (teardown! [this test]
        (.close conn))))))

(defn multiversion-checker
  "Ensures that every _version for a read has the *same* value."
  []
  (reify checker/Checker
    (check [_ test model history opts]
      (let [reads  (->> history
                        (filter op/ok?)
                        (filter #(= :read (:f %)))
                        (map :value)
                        (group-by :_version))
            multis (remove (fn [[k vs]]
                             (= 1 (count (set (map :value vs)))))
                           reads)]
        {:valid? (empty? multis)
         :multis multis}))))

(defn r [] {:type :invoke, :f :read, :value nil})
(defn w []
  (->> (iterate inc 0)
       (map (fn [x] {:type :invoke, :f :write, :value x}))
       gen/seq))

(defn an-test
  [opts]
  (merge tests/noop-test
         {:name    "crate"
          :db      (db)
          :client  (client)
          :checker (checker/compose
                     {:multi    (independent/checker (multiversion-checker))
                      :perf     (checker/perf)})
          :concurrency 100
          :nemesis (nemesis/partition-majorities-ring)
          :generator (->> (independent/concurrent-generator
                            10
                            (range)
                            (fn [id]
                              (->> (gen/reserve 5 (r) (w)))))
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 20)
                                             {:type :info, :f :start}
                                             (gen/sleep 200)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit 300))}
         opts))
