(ns jepsen.ignite
    (:require [clojure.tools.logging   :refer :all]
              [clojure.string          :as str]
              [jepsen [core            :as jepsen]
                      [checker         :as checker]
                      [cli             :as cli]
                      [client          :as client]
                      [control         :as c]
                      [db              :as db]
                      [generator       :as gen]
                      [tests           :as tests]
                      [independent     :as independent]
                      [util            :refer [timeout meh]]]
              [jepsen.control.util     :as cu]
              [jepsen.os.debian        :as debian]
              [jepsen.checker.timeline :as timeline]
              [knossos.model           :as model])
    (:import (java.io File)
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
        (c/sudo user
            (c/exec "bin/ignite.sh" (str server-dir "server-ignite-" node ".xml") :-v (c/lit (str ">" logfile " 2>&1 &")))
            (Thread/sleep 5000))))

(defn await-node-started
    "Waits for the grid cluster started."
    [node test]
    (info node "Waiting for topology snapshot")
    (let [tmp-file (File/createTempFile "jepsen-ignite-log" ".tmp")
        tmp-file-path (.getCanonicalPath tmp-file)]
    (try
        ; Download server log file
        (c/download logfile tmp-file-path)
        (while (str/blank? (re-find (re-pattern (str "Topology snapshot \\[.*?, servers\\=" (count (:nodes test)) ","))
            (slurp tmp-file-path))) (do
                (Thread/sleep 1000)
                (c/download logfile tmp-file-path)))
    (finally
        (.delete tmp-file)))))

(defn nuke!
    "Shuts down server and destroys all data."
    [node test]
    (c/su
        (meh (c/exec :pkill :-9 :-f "org.apache.ignite.startup.cmdline.CommandLineStartup"))
        (c/exec :rm :-rf server-dir))
    (info node "Apache Ignite nuked"))

(defn configure [addresses client-mode]
    "Creates a config file."
    (let [tmp-file (File/createTempFile "jepsen-ignite-config" ".xml")
          tmp-file-path (.getCanonicalPath tmp-file)]
        (spit tmp-file-path (clojure.string/replace (slurp "resources/apache-ignite.xml.tmpl")
            #"##addresses##|##instancename##|##clientmode##"
            { "##addresses##" (clojure.string/join "\n" (map #(str "\t<value>" % ":47500..47509</value>") addresses))
            "##instancename##" (.getName tmp-file)
            "##clientmode##" (str client-mode) }))
        tmp-file))

(defn configure-server [addresses node]
    "Creates a server config file and uploads it to the given node."
    (let [src-file (configure addresses false)]
        (c/upload src-file (str server-dir "server-ignite-" node ".xml"))
        (.delete src-file)))

(defn configure-client [addresses]
    "Creates a client config file."
    (configure addresses true))

(defn db
    "Apache Ignite cluster life-cycle."
    [version]
    (reify db/DB
        (setup! [_ test node]
            (info node "Installing Apache Ignite" version)
            (debian/install-jdk8!)
            (cu/ensure-user! user)
            (let [url (str "https://archive.apache.org/dist/ignite/" version "/apache-ignite-" version "-bin.zip")]
                (cu/install-archive! url server-dir))
            (configure-server (:nodes test) node)
            (c/exec :chown :-R (str user ":" user) server-dir)
            (start! node test)
            (await-node-started node test)
            (jepsen/synchronize test))

        (teardown! [_ test node]
            (nuke! node test))

        db/LogFiles
            (log-files [_ test node]
            [logfile])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 1000)})

(defrecord Client [conn cache config]
    client/Client
    (open! [this test node]
        (let [config (configure-client (:nodes test))
              conn (Ignition/start (.getCanonicalPath config))
              cache (.getOrCreateCache conn "JepsenCache")]
            (assoc this :conn conn :cache cache :config config)))

    (setup! [this test])

    (invoke! [_ test op]
        (try
            (case (:f op)
                :read (let [value (.get cache "k")]
                    (assoc op :type :ok :value value))
                :write (do (.put cache "k" (:value op))
                    (assoc op :type :ok)))))

    (teardown! [this test]
        (.delete config))

    (close! [_ test]
        (.stop conn true)))

(defn ignite-test
    "Given an options map from the command-line runner (e.g. :nodes, :ssh,
    :concurrency, ...), constructs a test map."
    [opts]
    (info :opts opts)
    (merge tests/noop-test
        opts
        {:name "ignite"
        :os debian/os
        :db (db "2.7.0")
        :client (Client. nil nil nil)
        :concurrency 3
        :checker (independent/checker
                 (checker/compose
                 {:linearizable (checker/linearizable {:model (model/cas-register)})
                  :timeline     (timeline/html)}))
        :generator (->> (gen/mix [r w])
            (gen/stagger 1/10)
            (gen/nemesis
                (gen/seq (cycle [(gen/sleep 5)
                    {:type :info, :f :start}
                    (gen/sleep 1)
                    {:type :info, :f :stop}])))
            (gen/time-limit 10))}))

(defn -main
    "Handles command line arguments. Can either run a test, or a web server for
    browsing results."
    [& args]
    (cli/run! (merge (cli/single-test-cmd {:test-fn ignite-test})
                     (cli/serve-cmd))
        args))
