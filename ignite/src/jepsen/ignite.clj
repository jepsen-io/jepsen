(ns jepsen.ignite
  (:require [clojure.tools.logging   :refer :all]
            [clojure.string          :as str]
            [clojure.java.io         :as io]
            [jepsen [core            :as jepsen]
                    [checker         :as checker]
                    [client          :as client]
                    [control         :as c]
                    [db              :as db]
                    [nemesis         :as nemesis]
                    [generator       :as gen]
                    [tests           :as tests]
                    [independent     :as independent]
                    [util            :refer [timeout meh]]]
            [jepsen.control.util     :as cu]
            [jepsen.os.debian        :as debian]
            [jepsen.checker.timeline :as timeline]
            [knossos.model           :as model])
  (:import (java.io File)
           (java.util UUID)
           (org.apache.ignite Ignition IgniteCache)
           (clojure.lang ExceptionInfo)))

(def user "ignite")
(def server-dir "/opt/ignite/")
(def logfile (str server-dir "node.log"))

(defn start!
  "Starts server for the given node."
  [node test]
  (info node "Starting server node")
    (c/cd server-dir
      (c/exec "bin/ignite.sh" (str server-dir "server-ignite-" node ".xml") :-v (c/lit (str ">" logfile " 2>&1 &")))))

(defn await-cluster-started
  "Waits for the grid cluster started."
  [node test]
  (while
    (= true (try (c/exec :egrep (c/lit (str "\"Topology snapshot \\[.*?, servers\\=" (count (:nodes test)) ",\"")) logfile)
      (catch Exception e true))) (do
        (info node "Waiting for cluster started")
        (Thread/sleep 3000))))

(defn nuke!
  "Shuts down server and destroys all data."
  [node test]
  (c/su
    (meh (c/exec :pkill :-9 :-f "org.apache.ignite.startup.cmdline.CommandLineStartup"))
    (c/exec :rm :-rf server-dir))
  (info node "Apache Ignite nuked"))

(defn configure [addresses client-mode]
  "Creates a config file."
  (-> (slurp "resources/apache-ignite.xml.tmpl")
    (str/replace "##addresses##" (clojure.string/join "\n" (map #(str "\t<value>" % ":47500..47509</value>") addresses)))
    (str/replace "##instancename##" (str (UUID/randomUUID)))
    (str/replace "##clientmode##" (str client-mode))))

(defn configure-server [addresses node]
  "Creates a server config file and uploads it to the given node."
  (c/exec :echo (configure addresses false) :> (str server-dir "server-ignite-" node ".xml")))

(defn configure-client [addresses]
  "Creates a client config file."
  (let [config-file (File/createTempFile "jepsen-ignite-config" ".xml")
        config-file-path (.getCanonicalPath config-file)]
    (spit config-file-path (configure addresses true))
     config-file))

(defn db
  "Apache Ignite cluster life-cycle."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "Installing Apache Ignite" version)
      (c/su
        (debian/install-jdk8!)
        (cu/ensure-user! user)
        (let [url (str "https://archive.apache.org/dist/ignite/" version "/apache-ignite-" version "-bin.zip")]
          (cu/install-archive! url server-dir))
        (c/exec :chown :-R (str user ":" user) server-dir))
      (c/sudo user
        (configure-server (:nodes test) node)
        (start! node test)
        (await-cluster-started node test)))

    (teardown! [_ test node]
      (nuke! node test))

    db/LogFiles
      (log-files [_ test node]
      [logfile])))

(defn get-cache-config
  [options]
  {:atomicity-mode   (:cache-atomicity-mode options)
   :cache-mode       (:cache-mode options)
   :write-sync-mode  (:cache-write-sync-mode options)
   :read-from-backup (:read-from-backup options)
   :backups          (:backups options)})

(defn get-transaction-config
  [options]
  {:concurrency      (:transaction-concurrency options)
   :isolation        (:transaction-isolation options)})

(defn generator
  [operations time-limit]
  (->> (gen/mix operations)
       (gen/stagger 1/10)
       (gen/nemesis
        (gen/seq (cycle [(gen/sleep 5)
          {:type :info, :f :start}
          (gen/sleep 1)
          {:type :info, :f :stop}])))
       (gen/time-limit time-limit)))

(defn checker
  [details]
  (checker/compose
    {:perf     (checker/perf)
     :timeline (timeline/html)
     :details  details}))

(defn basic-test
  "Sets up the test parameters common to all tests."
  [options]
  (info :opts options)
  (merge tests/noop-test
    (dissoc options
      :cache-atomicity-mode
      :cache-mode
      :cache-write-sync-mode
      :read-from-backup
      :transaction-concurrency
      :transaction-isolation
      :test-fns)
    {:name    "basic-test"
     :os      debian/os
     :db      (db (:version options))
     :nemesis (nemesis/partition-random-halves)}))
