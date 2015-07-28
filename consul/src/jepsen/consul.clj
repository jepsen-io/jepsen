(ns jepsen.consul
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
      (start-consul! test node)

      (Thread/sleep 1000)
      (info node "consul ready"))

    (teardown! [_ test node]
      (c/su
        (meh (c/exec :killall :-9 :consul))
        (c/exec :rm :-rf pidfile data-dir))
      (info node "consul nuked"))))

(defn maybe-int [value]
  (if (= value "null")
      nil
      (Integer. value)))

(defn parse-index [resp]
  (-> resp
      :headers
      (get "x-consul-index")
      Integer.))

(defn parse-body
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
  (let [body  (-> resp
                  :body
                  (json/parse-string #(keyword (.toLowerCase %)))
                  first)
        value (-> body :value base64/decode maybe-int)]
    (assoc body :value value)))

(defn parse [response]
  (assoc (parse-body response) :index (parse-index response)))

(defn consul-get [key-url]
  (http/get key-url))

(defn consul-put! [key-url value]
  (http/put key-url {:body value}))

(defn consul-cas! [key-url value new-value]
  "Consul uses an index based CAS so we must first get the existing value for
   this key and then use the index for a CAS!"
  (let [resp (parse (consul-get key-url))
        index (:index resp)
        existing-value (:value resp)]
    (if (= existing-value value)
        (let [params {:body new-value :query-params {:cas index}}
              body (:body (http/put key-url params))]
          (= (body "true")))
        false)))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (let [client (str "http://" (name node) ":8500/v1/kv/" k)]
      (consul-put! client (json/generate-string nil))
      (assoc this :client client)))

  (invoke! [this test op]
    (case (:f op)
      :read  (try (let [resp  (parse (consul-get client))]
                    (assoc op :type :ok :value (:value resp)))
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
