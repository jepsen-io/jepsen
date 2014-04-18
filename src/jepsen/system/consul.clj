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

(defn peer-addr [node]
  (str (name node) ":8300"))

(defn addr [node]
  (str (name node) ":8500"))

(defn peers
  "The command-line peer list for an consul cluster."
  [test]
  (->> test
       :nodes
       (map peer-addr)
       (str/join ",")))

(defn running?
  "Is consul running?"
  []
  (try
    (c/exec :start-stop-daemon :--status
            :--pidfile pidfile
            :--exec binary)
    true
    (catch RuntimeException _ false)))

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
  (let [running (atom nil)] ; A map of nodes to whether they're running
    (reify db/DB
      (setup! [this test node]
        (debian/install [:htop :psmisc])

        (c/su
          (c/cd "/opt"
                (when-not (cu/file? "consul")
                  (info node "cloning consul")
                  (c/exec (c/lit "mkdir consul"))))

          ; Initially, every node is not running.
          (core/synchronize test)
          (reset! running (->> test :nodes (map #(vector % false)) (into {})))

          ; All nodes synchronize at each startup attempt.
          (while (do (core/synchronize test)
                     (when (= node (core/primary test))
                       (info "Running nodes:" @running))
                     (not-every? @running (:nodes test)))

            ; Nuke local node; we've got to start fresh if we got ourselves
            ; wedged last time.
            (db/teardown! this test node)
            (core/synchronize test)

            ; Launch primary first
            (when (= node (core/primary test))
              (start-consul! test node)
              (Thread/sleep 1000))

            ; Launch secondaries in any order after the primary...
            (core/synchronize test)

            ; ... but with some time between each to avoid triggering the join
            ; race condition
            (when-not (= node (core/primary test))
              (locking running
                (Thread/sleep 1000)
                (start-consul! test node)))

            ; Good news is these puppies crash quick, so we don't have to
            ; wait long to see whether they made it.
            (Thread/sleep 2000)
            (swap! running assoc node (running?)))

          (info node "consul ready")))

      (teardown! [_ test node]
        (c/su
          (meh (c/exec :killall :-9 :consul))
          (c/exec :rm :-rf pidfile data-dir))
        (info node "consul nuked")))))

(defn parse-index
  [resp]
  (let [h (:headers resp)
        index (ccore/get h "X-Consul-Index")]
    index))

(defn maybe-int
  [value]
  (if (= value "null") nil (Integer. value)))

(defn parse-value
  [resp]
  (let [value (json/parse-string (:body resp) true)
     seriousValue (-> (first value)
                             :Value
                             base64/decode
                             maybe-int)]
    seriousValue))

(defn consul-get
  [key-url]
  (http/get key-url))

(defn consul-put
  [key-url value]
  (http/put key-url {:body value}))

(defn attempt-cas
  [key-url index new_val]
  (let [resp (http/put key-url {:body new_val :query-params {:cas index}})
        body (:body resp)]
        (= body "true")))

(defn consul-cas
  [key-url value new-val]
  (let [resp (consul-get key-url)
        index (parse-index resp)
        exist (parse-value resp)]
    (if (= exist value) (attempt-cas key-url index new-val) false)))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (let [client (str "http://" (name node) ":8500/v1/kv/" k)]
      (consul-put client (json/generate-string nil))
      (assoc this :client client)))

  (invoke! [this test op]
    (try+
      (case (:f op)
        :read  (try (let [value (parse-value (consul-get client))]
                      (assoc op :type :ok :value value))
                    (catch Exception e
                      ;(warn e "Read failed")
                      ; Since reads don't have side effects, we can always
                      ; pretend they didn't happen.
                      (assoc op :type :fail)))

        :write (try (do (->> (:value op)
                        json/generate-string
                        (consul-put client))
                   (assoc op :type :ok))
                  (catch Exception e
                      ;(warn e "Write failed")
                      ; Since reads don't have side effects, we can always
                      ; pretend they didn't happen.
                      (assoc op :type :fail)))

        :cas   (try
                   (let [[value value'] (:value op)
                       ok?            (consul-cas client
                                              (json/generate-string value)
                                              (json/generate-string value'))]
                   (assoc op :type (if ok? :ok :fail)))
                (catch Exception e
                    ;(warn e "CAS failed")
                    ; Since reads don't have side effects, we can always
                    ; pretend they didn't happen.
                    (assoc op :type :fail))))

      (catch (and (:errorCode %) (:message %)) e
        (assoc op :type :info :value e))))

  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single consul node."
  []
  (CASClient. "jepsen" nil))
