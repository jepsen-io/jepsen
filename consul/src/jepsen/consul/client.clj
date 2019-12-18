(ns jepsen.consul.client
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.string :as str]
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
      (get "X-Consul-Index")
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
  (assoc (parse-body response)
         :index (parse-index response)))

(defn get
  ([url]
   (http/get url))
  ([url key]
   (http/get (str url key))))

(defn put! [url key value]
  (http/put (str url key) {:body value}))

(defn cas!
  "Consul uses an index based CAS so we must first get the existing value for
   this key and then use the index for a CAS!"
  [url key value new-value]
  (let [res (parse (get url key))
        existing-value (str (:value res))
        index (:index res)]
    (if (= existing-value value)
      (let [params {:body new-value :query-params {:cas index}}
            body (:body (http/put (str url key) params))]
        (= body "true"))
      false)))

(defn txn
  "TODO"
  [])

(defmacro with-errors
  [op idempotent & body]
  `(try ~@body
        (catch Exception e#
            (let [type# (if (~idempotent (:f ~op))
                         :fail
                         :info)]
              (condp re-find (.getMessage e#)
                #"404" (assoc ~op :type type# :error :key-not-found)
                #"500" (assoc ~op :type type# :error :server-unavailable)
                (throw e#))))))

;; FIXME simplify this whole thing, it's repetitive
(defn await-cluster-ready
  "Blocks until cluster index matches count of nodes on test"
  [node count]

  ;; FIXME Maybe we use the index count here?
  (let [url (str "http://" (net/ip (name node)) ":8500/v1/catalog/nodes?index=1")]
     (with-retry [attempts 5]
       (get url)

       ;; We got an application-level response, let's proceed!
       (catch clojure.lang.ExceptionInfo e
         (if (and (= (.getMessage e) "clj-http: status 500")
                  (< 0 attempts))
           (retry (dec attempts))
           (throw+ {:error :retry-attempts-exceeded :exception e})))

       ;; Cluster not converged yet, let's keep waiting
       (catch java.net.ConnectException e
         (if (< 0 attempts)
           (do
             ;; TODO It would be nice to remove this log warning if we don't have connection issues anymore
             (warn "Connection refused from node:" node ", retrying. Attempts remaining:" attempts)
             (Thread/sleep 1000)
             (retry (dec attempts)))
           (throw+ {:error :retry-attempts-exceeded :exception e})))))
  true)
