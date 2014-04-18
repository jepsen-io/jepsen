(ns jepsen.system.consul
  (:require [clojure.tools.logging    :refer [debug info warn]]
            [clojure.java.io          :as io]
            [clojure.string           :as str]
            [jepsen.core              :as core]
            [jepsen.util              :refer [meh timeout]]
            [jepsen.core              :as core]
            [jepsen.control           :as c]
            [jepsen.control.net       :as net]
            [jepsen.control.util      :as cu]
            [jepsen.client            :as client]
            [jepsen.db                :as db]
            [cheshire.core            :as json]
            [clj-http.client          :as http]
            [base64-clj.core          :as base64]))

(def binary "/usr/bin/consul")
(def pidfile "/var/run/consul.pid")
(def data-dir "/var/lib/consul")
(def log-file "/var/log/consul.log")

(defn start-consul!
  [test node]
  (info node "starting consul")
  (c/exec :start-stop-daemon :--start
          :--background
          :--make-pidfile
          :--pidfile        pidfile
          :--chdir          "/opt/consul"
          :--exec           binary
          :--no-close
          :--
          :agent
          :-server
          :-log-level       "debug"
          :-client          "0.0.0.0"
          :-bind            (net/ip (name node))
          :-data-dir        data-dir
          :-node            (name node)
          (when (= node (core/primary test)) :-bootstrap)
          (when-not (= node (core/primary test))
            [:-join        (net/ip (name (core/primary test)))])
          :>>               log-file
          (c/lit "2>&1")))

(defn db []
  (reify db/DB
    (setup! [this test node]
      ; bring down any lingering processes
      (db/teardown! this test node)
      (start-consul! test node)

      (Thread/sleep 1000)
      (info node "consul ready"))

    (teardown! [_ test node]
      (c/su
        (meh (c/exec :killall :-9 :consul))
        (c/exec :rm :-rf pidfile data-dir))
      (info node "consul nuked"))))

(defn parse-index [resp]
  (-> resp
      :headers
      (get "x-consul-index")
      Integer.))

(defn maybe-int [value]
  (if (= value "null")
      nil
      (Integer. value)))

(defn parse-value
  "Parse the base64 encoded value.
   The response JSON looks like:
    [
     {
       \"CreateIndex\": 100,
       \"ModifyIndex\": 200,
       \"Key\": \"foo\",
       \"Flags\": 0,
       \"Value\": \"YmFy\"
     }
    ]
  "
  [resp]
  (-> resp
      :body
      (json/parse-string true)
      first
      :Value
      base64/decode
      maybe-int))

(defn consul-get [key-url]
  (http/get key-url))

(defn consul-put! [key-url value]
  (http/put key-url {:body value}))

(defn attempt-cas
  [key-url index new_val]
  (let [resp (http/put key-url {:body new_val :query-params {:cas index}})
        body (:body resp)]
        (= body "true")))

(defn consul-cas!
  [key-url value new-val]
  (let [resp (consul-get key-url)
        index (parse-index resp)
        exist (parse-value resp)]
    (if (= exist value) (attempt-cas key-url index new-val) false)))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (let [client (str "http://" (name node) ":8500/v1/kv/" k)]
      (consul-put! client (json/generate-string nil))
      (assoc this :client client)))

  (invoke! [this test op]
    (case (:f op)
      :read  (try (let [value (parse-value (consul-get client))]
                    (assoc op :type :ok :value value))
                  (catch Exception e
                    (warn e "Read failed")
                    ; Since reads don't have side effects, we can always
                    ; pretend they didn't happen.
                    (assoc op :type :fail)))

      :write (do (->> (:value op)
                      json/generate-string
                      (consul-put! client))
                 (assoc op :type :ok))

      :cas   (let [[value value'] (:value op)
                   ok?            (consul-cas! client
                                               (json/generate-string value)
                                               (json/generate-string value'))]
               (assoc op :type (if ok? :ok :fail)))))

  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single consul node."
  []
  (CASClient. "jepsen" nil))
