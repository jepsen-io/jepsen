(ns jepsen.consul.client
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.control.net :as net]
            [base64-clj.core :as base64]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [dom-top.core :refer [with-retry]]
            [slingshot.slingshot :refer [try+ throw+]]))

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

(defn consul-cas!
  "Consul uses an index based CAS so we must first get the existing value for
   this key and then use the index for a CAS!"
  [key-url value new-value]
  (let [resp (parse (consul-get key-url))
        index (:index resp)
        existing-value (:value resp)]
    (if (= existing-value value)
        (let [params {:body new-value :query-params {:cas index}}
              body (:body (http/put key-url params))]
          (= (body "true")))
        false)))

;; TODO Add with-errors classification
;; TODO Make compatible with jepsen.independent
(defrecord CASClient [k client]
  client/Client
  (open! [this test node]
    (let [client (str "http://" (net/ip (name node)) ":8500/v1/kv/" k)]
      (consul-put! client (json/generate-string nil))
      (assoc this :client client)))

  (invoke! [this test op]
    (case (:f op)
      :read  (try (let [resp  (parse (consul-get client))]
                    (assoc op :type :ok :value (:value resp)))
                  (catch Exception e
                    (assoc op :type :fail :error :read-failed)))

      :write (do (->> (:value op)
                      json/generate-string
                      (consul-put! client))
                 (assoc op :type :ok))

      :cas   (let [[value value'] (:value op)
                   ok?            (consul-cas! client
                                               (json/generate-string value)
                                               (json/generate-string value'))]
               (assoc op :type (if ok? :ok :fail)))))

  (close! [this _]
    (assoc this :client nil))

  (setup! [_ _])
  (teardown! [_ _]))

(defn cas-client
  "A compare and set register built around a single consul node."
  []
  (CASClient. "jepsen" nil))

(defn await-cluster-ready
  "Blocks until cluster index matches count of nodes on test"
  [node count]

  ;; FIXME Maybe we use the index count here?
  (let [url (str "http://" (net/ip (name node)) ":8500/v1/catalog/nodes?index=1")
        #_(str "http://" (net/ip (name node)) ":8500/v1/catalog/nodes?index=" count)
        ]
    (with-retry [attempts 50]
      (try+
       ;; FIXME Take out this log
       (info (consul-get url))

       ;; This tells us that the server is responding and is ready
       (catch [:message "clj-http: status 500"] e
         ;; FIXME Probably don't need this log
         (warn e))

       ;; Cluster not converged yet, let's keep waiting
       (catch java.net.ConnectException e
         (if (< 0 attempts)
           (do
             ;; TODO It would be nice to remove this log warning if we don't have connection issues anymore
             (warn "Connection refused from node:" node ", retrying. Attempts remaining:" attempts)
             (Thread/sleep (rand 1000))
             (retry (dec attempts)))
           (throw e))))))
  true)
