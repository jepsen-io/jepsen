(ns jepsen.raftis
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [taoensso.carmine :as car :refer (wcar)]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(defn client
  "A client for a single register"
  [conn]
  (reify client/Client
    (open! [this test node] this)

    (setup! [_ test node]
      (client {:pool {} :spec {:host (name node) :port 6379 :timeout-ms 5000}}))

    (invoke! [this test op]
      (try
        (case (:f op)
          :read (assoc op :type :ok, :value (parse-long (get (car/wcar conn (car/get "r")) 0 "")))

          :write (do (car/wcar conn (car/set "r" (:value op)))
                     (assoc op :type, :ok)))

        (catch clojure.lang.ExceptionInfo e
          (let [err_str (str (.getMessage e))]
            (let [no_leader (re-find #"ERR write InComplete: no leader node!.*" err_str)]
              (let [socket_closed (re-find #"socket closed.*" err_str)]
                (assoc op :type (if (or (= :read (:f op)) no_leader socket_closed) :fail :info), :error err_str)))))

        (catch java.net.SocketTimeoutException e
          (assoc op :type (if (= :read (:f op)) :fail :info), :error :timeout))

        (catch java.io.EOFException e
          (assoc op :type :fail, :error :eof_exception))

        (catch java.lang.NumberFormatException e
          (assoc op :type :fail, :error (str "readnil--- " e)))))

    (teardown! [_ test])

    (close! [_ test])))

(def dir     "/opt/raftis")
(def logfile (str dir "/data/LOG"))
(def logfile1 (str dir "/raftis.log"))
(def pidfile (str dir "/raftis.pid"))
(def binary  "raftis")

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
    \"192.168.1.2:8901,192.168.1.3:8901,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str (name node) ":8901")))
       (str/join ",")))

(defn db
  "Raftis DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing raftis" version)
        (let [url (str "https://github.com/PikaLabs/floyd/releases/download/"
                       version "/raftis-" version ".tar.gz")]
          (cu/install-archive! url dir))
        (cu/start-daemon!
          {:logfile logfile1
           :pidfile pidfile
           :chdir dir}
          binary
          (initial-cluster test)
          node
          "8901"
          "data"
          "6379")
        (Thread/sleep 10000)))

    (teardown! [_ test node]
      (info node "tearing down raftis")
      (c/su
        (cu/stop-daemon! binary pidfile)
        (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn raftis-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "raftis"
          :os debian/os
          :db (db "v2.0.4")
          :client (client nil)
          :nemesis (nemesis/partition-random-halves)
          :model (model/register 0)
          :checker (checker/compose
                     {:perf     (checker/perf)
                      :timeline (timeline/html)
                      :linear   (checker/linearizable)})
          :generator (->> (gen/mix [r w])
                          (gen/stagger 1/10)
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 5)
                                             {:type :info, :f :start}
                                             (gen/sleep 5)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn raftis-test})
                   (cli/serve-cmd))
            args))
