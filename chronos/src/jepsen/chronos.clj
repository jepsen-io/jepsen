(ns jepsen.chronos
  "Sets up chronos"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clj-http.client :as http]
            [clj-time.core :as time]
            [clj-time.format :as time.format]
            [cheshire.core :as json]
            [jepsen [client :as client]
             [core :as jepsen]
             [db :as db]
             [tests :as tests]
             [control :as c :refer [|]]
             [checker :as checker]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :refer [timeout meh]]
             [mesosphere :as mesosphere]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.chronos.checker :refer [checker epsilon-forgiveness]]))

(def port "docs say 8080 but the package binds to 4400 by default wooo" 4400)
(def job-dir "/tmp/chronos-test/")

(defn uri
  "Constructs a chronos URI for a given path."
  [node & path]
  (str "http://" (name node) ":" port "/" (str/join "/" (map name path))))

(defn jobs
  "What jobs are there?"
  [node]
  (->> (http/get (uri node :scheduler :jobs)
                 {:as :json})
       :body))

(defn configure
  [test node]
  (c/su
    ; Lower the scheduler horizon; otherwise Chronos will just forget to run
    ; tasks that happen frequently
    (c/exec :echo "1" :> "/etc/chronos/conf/schedule_horizon")))

(defn start!
  "Starts chronos if it's not already running."
  [test node]
  (c/su
    (try (c/exec :service :chronos :status)
         (catch RuntimeException e
           (info node "starting chronos")
           (c/exec :service :chronos :start)))))

(defn db
  "Sets up and tears down Chronos. You can get versions from

      curl http://repos.mesosphere.com/debian/dists/wheezy/main/binary-amd64/Packages.bz2 | bunzip2 | egrep '^Package:|^Version:' | paste - - | sort"
  [mesos-version chronos-version]
  (let [mesosphere (mesosphere/db mesos-version)]
    (reify db/DB
      (setup! [_ test node]
        (db/setup! mesosphere test node)

        (debian/install {:chronos chronos-version})
        (configure test node)
        (c/su (c/exec :mkdir :-p job-dir))

        (start! test node))

      (teardown! [_ test node]
        (info node "stopping chronos")
        (c/su (meh (c/exec :service :chronos :stop))
              (meh (cu/grepkill "/usr/bin/chronos")))
        (db/teardown! mesosphere test node)
        (c/su (c/exec :rm :-rf job-dir)
              (c/exec :truncate :--size 0 "/var/log/messages")))

      db/LogFiles
      (log-files [_ test node]
        (concat (db/log-files mesosphere test node)
                ["/var/log/messages"])))))

(defn now-str
  []
  (time.format/unparse (time.format/formatters :date-time)
                       (time/now)))

; Job representation
;
; Jobs are maps with the following keys:
; :name     - A globally unique name for the job
; :start    - A datetime for when the job begins
; :interval - How long between runs in seconds
; :count    - How many times should we repeat the job
; :epsilon  - Allowable tolerance, in seconds, for scheduling
; :duration - How long should a run take, in seconds?

(def formatter (time.format/formatters :date-time))

(defn interval-str
  "Given a job, emits an ISO8601 repeating interval representation."
  [job]
  (str "R" (:count job) "/"
       (time.format/unparse formatter (:start job))
       "/PT" (:interval job) "S"))

(defn command
  "Given a job, constructs a shell command that picks a tempfile and logs the
  job name, invocation time, sleeps, then logs the completion time."
  [job]
  (str "MEW=$(mktemp -p " job-dir "); "
       "echo \"" (:name job) "\" >> $MEW; "
       "date -u -Ins >> $MEW; "
       "sleep " (:duration job) "; "
       "date -u -Ins >> $MEW;"))

(defn job->json
  "Transforms a job into a JSON string suitable for sending to Chronos."
  [job]
  (json/generate-string
    {:name              (:name job)
     :command           (command job)
     :schedule          (interval-str job)
     :scheduleTimeZone  "UTC"
     :owner             "jepsen@jepsen.io"
     :epsilon           (str "PT" (:epsilon job) "S")
     :mem               1
     :disk              1
     :cpus              0.001
     :async             false}))

(defn add-job!
  "Submits a new job to Chronos. See
  https://mesos.github.io/chronos/docs/api.html."
  [node job]
  (http/post (uri node :scheduler :iso8601)
             {:content-type :json
              :body (job->json job)}
             {:as :json}))

(defn parse-file-time
  "Date can (maybe depending on locale) emit datetimes with commas to separate
  fractional seconds, which (even though it's valid ISO8601) confuses
  clj-time, so we substitute dots for commas before parsing."
  [t]
  (when t
    (time.format/parse formatter (str/replace t \, \.))))

(defn parse-file
  "Given a node name and a run logfile with a name, start time, and end time,
  returns a map for that run."
  [node file-str]
  (let [[name start end] (str/split file-str #"\n")]
    {:node  node
     :name  (Long/parseLong name)
     :start (parse-file-time start)
     :end   (parse-file-time end)}))

(defn read-runs
  "Returns all runs from all nodes."
  [test]
  (->> (c/on-many
         (:nodes test)
         (->> (cu/ls-full job-dir)
              (pmap (partial c/exec :cat))
              (mapv (partial parse-file c/*host*))))
       vals
       (reduce concat)))

(defrecord Client [node]
  client/Client
  (setup! [this test node]
    (assoc this :node node))

  (invoke! [this test op]
    (timeout 20000 (assoc op :type :info, :value :timed-out)
             (try
               (case (:f op)
                 :add-job (do (add-job! node (:value op))
                              (assoc op :type :ok))
                 :read    (do ; (info (with-out-str (pprint (jobs node))))
                              (assoc op
                                     :type :ok
                                     :value (read-runs test))))
               (catch org.apache.http.ConnectionClosedException e
                 (assoc op :type :fail, :value (.getMessage e)))
               (catch java.net.ConnectException e
                 (assoc op :type :fail, :value (.getMessage e))))))

  (teardown! [_ test]))

(defn add-job
  "Generator for creating new jobs."
  []
  (let [id (atom 0)]
    (reify gen/Generator
      (op [_ test process]
        (let [head-start  10 ; Schedule a bit in the future
              duration    (rand-int 10)
              epsilon     (+ 10 (rand-int 20))
              ; Chronos won't schedule tasks concurrently, so we ensure they'll
              ; never overlap.
              interval    (+ 1
                             duration
                             epsilon
                             epsilon-forgiveness
                             (rand-int 30))]
        {:type   :invoke
         :f      :add-job
         :value  {:name     (swap! id inc)
                  :start    (time/plus (time/now) (time/seconds head-start))
                  :count    (inc (rand-int 99))
                  :duration duration
                  :epsilon  epsilon
                  :interval interval}})))))

(defn resurrection-hub
  "Mesos and Chronos like to crash all the time. We have to bring them back to
  life regularly. Wraps another nemesis."
  [nemesis]
  (reify client/Client
    (setup! [this test node]
      (resurrection-hub (client/setup! nemesis test node)))

    (invoke! [this test op]
      (if (not= :resurrect (:f op))
        (client/invoke! nemesis test op)
        (do (c/on-many (:nodes test)
                       (let [node (keyword c/*host*)]
                         (mesosphere/start-master! test node)
                         (mesosphere/start-slave! test node)
                         (start! test node)))
            (assoc op :value :resurrection-complete))))

    (teardown! [this test]
      (client/teardown! nemesis test))))

(defn simple-test
  "Create some jobs, let em run, and do a final read to see which ran."
  [mesos-version chronos-version]
  (assoc tests/noop-test
         :name      "chronos"
         :os        debian/os
         :db        (db mesos-version chronos-version)
         :client    (->Client nil)
         :generator (gen/phases
                      (->> (add-job)
                           (gen/delay 30)
                           (gen/stagger 30)
                           (gen/nemesis
                             (gen/seq (cycle [(gen/sleep 200)
                                              {:type :info, :f :start}
                                              (gen/sleep 200)
                                              {:type :info, :f :stop}
                                              {:type :info, :f :resurrect}])))
                           (gen/time-limit 450))
                      (gen/nemesis (gen/once {:type :info, :f :stop}))
                      (gen/nemesis (gen/once {:type :info, :f :resurrect}))
                      (gen/log "Waiting for executions")
                      (gen/sleep 400)
                      (gen/clients
                             (gen/once
                               {:type :invoke, :f :read})))
         :nemesis   (resurrection-hub
                      (nemesis/partition-random-halves))
         :checker   (checker/compose
                      {:chronos (checker)
                       :perf (checker/perf)})))
