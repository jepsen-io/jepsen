(ns jepsen.consul
  (:gen-class)
  (:use jepsen.core
        jepsen.tests
        clojure.test
        clojure.pprint)
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.core :as core]
            [jepsen.util :as util :refer [meh timeout]]
            [jepsen.control :as c]
            [jepsen.control.net :as net]
            [jepsen.control.util :as cu]
            [jepsen.client :as client]
            [jepsen.cli :as cli]
            [jepsen.db :as db]
            [jepsen.os.debian :as debian]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.store :as store]
            [jepsen.report :as report]
            [jepsen.tests :as tests]
            [knossos.model :as model]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [base64-clj.core :as base64]))

(def dir "/opt/consul")
(def binary "consul")
(def pidfile "/var/run/consul.pid")
(def data-dir "/var/lib/consul")
(def logfile "/var/log/consul.log")

(defn start-consul!
  [test node]
  (info node "starting consul")

  ;; TODO Port this over to start-daemon, or whatever Kyle is using in latest jepsen
  #_(cu/start-daemon!
   {:logfile logfile
    :pidfile pidfile
    :chdir   dir}
   binary
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
   :>>               logfile
   (c/lit "2>&1"))

  (c/exec :start-stop-daemon :--start
          :--background
          :--make-pidfile
          :--pidfile        pidfile
          :--chdir          "/opt"
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
          :>>               logfile
          (c/lit "2>&1"))
  )

(defn db
  "Install consul specific version"
  [version]
  (reify db/DB
    (setup! [this test node]
      (info node "installing consul" version)
      (c/su
       (let [url (str "https://releases.hashicorp.com/consul/"
                      version "/consul_" version "_linux_amd64.zip")]
         (cu/install-archive! url dir)))
      (start-consul! test node)

      (Thread/sleep 1000)
      (info node "consul ready"))

    (teardown! [_ test node]
      (c/su
       (info node "consul killed")
       (cu/stop-daemon! binary pidfile)

       (info node "consul data cleared")
       (c/exec :rm :-rf pidfile data-dir)

       (info node "consul bin removed")
       (c/su
        (c/exec :rm :-rf dir)))
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
    (let [client (str "http://" (net/ip (name node)) ":8500/v1/kv/" k)]
      (consul-put! client (json/generate-string nil))
      (assoc this :client client)))

  ;; TODO catch the status 500 process crashes
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


;; Port this to jepsen.tests.linearizable_register
(defn register-test
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         {:name      "consul"
          :os        debian/os
          :db        (db "1.6.1")
          :client    (cas-client)
          ;; TODO Lift this to an independent key checker
          :checker   (checker/compose
                      {:perf     (checker/perf)
                       :timeline (timeline/html)
                       :linear (checker/linearizable
                                {:model (model/cas-register)})})
          :nemesis   (nemesis/partition-random-halves)
          :generator (gen/phases
                      (->> gen/cas
                           (gen/delay 1/2)
                           (gen/nemesis
                            (gen/seq
                             (cycle [(gen/sleep 10)
                                     {:type :info :f :start}
                                     (gen/sleep 10)
                                     {:type :info :f :stop}])))
                           (gen/time-limit 120))
                      (gen/nemesis
                       (gen/once {:type :info :f :stop}))
                                        ; (gen/sleep 10)
                      (gen/clients
                       (gen/once {:type :invoke :f :read})))}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn register-test})
                   (cli/serve-cmd))
            args))

