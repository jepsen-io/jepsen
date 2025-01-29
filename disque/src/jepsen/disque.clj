(ns jepsen.disque
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh
                                                timeout
                                                relative-time-nanos]]
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
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [knossos.core :as knossos])
  (:import (clojure.lang ExceptionInfo)
           (java.net URI)
           (com.github.xetorthio.jedisque Jedisque
                                          JobParams)
           (redis.clients.jedis.exceptions JedisConnectionException
                                           JedisDataException)))

(def dir          "/opt/disque")
(def data-dir     "/var/lib/disque")
(def pidfile      "/var/run/disque.pid")
(def binary       (str dir "/src/disque-server"))
(def control      (str dir "/src/disque"))
(def config-file  (str dir "/disque.conf"))
(def log-file     (str data-dir "/log"))
(def port         7711)

(defn install!
  "Installs DB on the given node."
  [node version]
  (info node "installing disque")
  (debian/install [:git-core :build-essential])
  (c/su
    (c/cd "/opt"
          (when-not (cu/file? "disque")
            (c/exec :git :clone "https://github.com/antirez/disque.git")))
    (c/cd dir
          (c/exec :git :pull)
          (c/exec :git :reset :--hard version)
          (c/exec :make))
    ))

(defn configure!
  "Uploads configuration files to the given node."
  [node test]
  (c/su
    (info node "configuring")
    (c/exec :echo
            (-> "disque.conf" io/resource slurp
                (str/replace #"%DATA_DIR%" data-dir))
            :> config-file)))

(defn running?
  "Is the service running?"
  []
  (try
    (c/exec :start-stop-daemon :--status
            :--pidfile pidfile
            :--exec    binary)
    true
    (catch RuntimeException _ false)))

(defn start!
  "Starts DB."
  [node test]
  (info node "starting disque")
  (c/su
    (assert (not (running?)))
    (c/exec :mkdir :-p data-dir)
    (c/exec :start-stop-daemon :--start
            :--background
            :--make-pidfile
            :--pidfile  pidfile
            :--chdir    dir
            :--exec     binary
            :--no-close
            :--
            config-file
            :>>         log-file
            (c/lit "2>&1"))
    (info node "started")))

(defn join!
  "Joins cluster nodes together."
  [node test]
  ; Join everyone to primary
  (jepsen/synchronize test)
  (let [p (jepsen/primary test)]
    (when-not (= node p)
      (info node "joining" p)
      (let [res (c/exec control "-p" port
                        :cluster :meet (net/ip (name p)) port)]
        (assert (re-find #"^OK$" res))))))

(defn stop!
  "Stops DB."
  [node]
  (info node "stopping disque")
  (c/su
    (meh (c/exec :killall :-9 :disque-server))
    (meh (c/exec :rm :-rf pidfile))))

(defn wipe!
  "Shuts down the server and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su (meh (c/exec :rm :-rf (c/lit (str data-dir "/*")) log-file))))

(defn db [version]
  "Disque for a particular version."
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)
        (configure! test)
        (start! test)
        (join! test)))

    (teardown! [_ test node]
      (wipe! node))

    db/LogFiles
    (log-files [_ _ _] [log-file])))

; Client

; Gonna have to generalize disque a bit, and its type signatures are awkward
(defprotocol Disque
  (add-job! [client queue body timeout params])
  (get-job! [client timeout count queues])
  (ack-job! [client job-ids]))

(extend-protocol Disque
  Jedisque
  (add-job! [client queue body timeout params]
    (.addJob client queue body timeout params))
  (get-job! [client timeout count queues]
    (.getJob client timeout count (into-array queues)))
  (ack-job! [client job-ids]
    (.ackjob client (into-array job-ids))))

(defmacro with-conn-errors
  "For use by reconnecting-client: takes an error-handling fn and evaluates
  `body`; if a JedisConnectionException is thrown, invokes the fn and
  rethrows."
  [f & body]
  `(try ~@body
     (catch JedisConnectionException e#
       (~f)
       (throw e#))))

(defn reconnecting-client
  "For some reason, I can't get jedisque to ever recover from a server restart.
  This wrapper asynchronously replaces dead clients with fresh ones, like a
  parent surreptiously replacing your goldfish as they pass away. They don't
  stop exceptions from being raised though. The process of loss is still
  important.

  Also your parents are poisoning the tank's water supply, muttering something
  about \"testing\". You may want to call social services."
  [uris]
  (let [make-client #(Jedisque. (into-array uris))
        replacing?  (atom false)
        client      (atom (make-client))
        replace!    (fn replace! []
                      (when (compare-and-set! replacing? false true)
                        (future
                          (try
                            (reset! client (make-client))
                            (catch Throwable t
                              (info t "Couldn't reconnect"))
                            (finally
                              (reset! replacing? false))))))]

    (reify Disque
      (add-job! [_ q b t p] (with-conn-errors replace!
                              (add-job! @client q b t p)))
      (get-job! [_ t c q] (with-conn-errors replace! (get-job! @client t c q)))
      (ack-job! [_ js]    (with-conn-errors replace! (ack-job! @client js))))))

(defn dequeue!
  "Given a client and an :invoke :dequeue op, dequeues and acks a job."
  [client queue client-timeout op]
  (let [job (-> client
                (get-job! client-timeout 1 [queue])
                first)]
    (if (nil? job)
      ; Nothing to dequeue.
      (assoc op :type :fail)

      ; Ack job and return.
      (do (ack-job! client [(.getId job)])
          (assoc op
                 :type  :ok
                 :value (Long/parseLong (.getBody job)))))))

(defrecord Client [^Jedisque client queue client-timeout job-params]
  client/Client
  (setup! [this test node]
    (let [uri (URI. (str "disque://" (name node) ":7711"))]
      (assoc this :client (reconnecting-client [uri]))))

  (invoke! [this test op]
    (try
      (case (:f op)
        :enqueue (do
                   (add-job! client queue (str (:value op)) client-timeout
                             job-params)
                   (assoc op :type :ok))
        :dequeue (dequeue! client queue client-timeout op)
        :drain   (timeout 10000 (assoc op :type :info :value :timeout)
                          (loop []
                            (let [op' (->> (assoc op
                                                  :f    :dequeue
                                                  :time (relative-time-nanos))
                                           util/log-op
                                           (jepsen/conj-op! test)
                                           (dequeue!
                                             client queue client-timeout))]
                              ; Log completion
                              (->> (assoc op' :time (relative-time-nanos))
                                   util/log-op
                                   (jepsen/conj-op! test))

                              (if (= :fail (:type op'))
                                ; Done
                                (assoc op :type :ok, :value :exhausted)

                                ; Keep going.
                                (recur))))))
      (catch JedisDataException e
        (if (re-find #"^NOREPL" (.getMessage e))
          (assoc op :type :info, :value :not-fully-replicated)
          (throw e)))
      (catch JedisConnectionException e
        (assoc op :type :info, :value (.getMessage e)))))

  (teardown! [this test]
    (.close client)))

(defn client
  []
  (Client. nil
           "jepsen"
           100
           (-> (JobParams.)
               (.setRetry (int 1))
               (.setReplicate (int 3)))))

; Nemeses

(defn killer
  "Kills a random node on start, restarts it on stop."
  []
  (nemesis/node-start-stopper
    rand-nth
    (fn start [test node] (stop! node))
    (fn stop  [test node] (start! node test))))

; Generators

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  [gen]
  (gen/phases
    (->> gen
         (gen/nemesis
           (gen/seq (cycle [(gen/sleep 10)
                            {:type :info :f :start}
                            (gen/sleep 10)
                            {:type :info :f :stop}])))
         (gen/time-limit 100))
    ; Recover
    (gen/nemesis (gen/once {:type :info :f :stop}))
    ; Wait for resumption of normal ops
    (gen/clients (gen/time-limit 10 gen))
    ; Drain
    (gen/log "Draining")
    (gen/clients (gen/each (gen/once {:type :invoke
                                      :f    :drain})))))

; Tests

(defn disque-test
  [name opts]
  (merge tests/noop-test
         {:name    (str "disque " name)
          :client  (client)
          :os      debian/os
          :db      (db "f00dd0704128707f7a5effccd5837d796f2c01e3")
          :model   (model/unordered-queue)
          :generator (->> (gen/queue)
                          (gen/delay 1)
                          std-gen)
          :checker (checker/compose {:queue       checker/total-queue
                                     :latency     (checker/latency-graph)})}
         opts))

(defn single-node-restarts-test
  []
  (disque-test "single node restarts"
               {:nemesis   (killer)}))

(defn partitions-test
  []
  (disque-test "partitions"
               {:nemesis (nemesis/partition-random-halves)}))
