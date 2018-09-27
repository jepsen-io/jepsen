(ns jepsen.faunadb.client
  "A clojure client for FaunaDB"
  (:import (com.faunadb.client FaunaClient)
           (com.faunadb.client.types Codec
                                     Decoder
                                     Field
                                     Value
                                     Value$ObjectV
                                     Value$ArrayV
                                     Value$LongV
                                     Types))
  (:require [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [jepsen.util :as util]
            [jepsen.faunadb.query :as q]))

(def root-key
  "Administrative key for the FaunaDB cluster."
  "secret")

(def BoolField
  (Field/as Codec/BOOLEAN))

(def LongField
  (Field/as Codec/LONG))

(defn client
  "Constructs a Fauna client"
  [node]
  (.build (doto (FaunaClient/builder)
            (.withEndpoint (str "http://" node ":8443"))
            (.withSecret root-key))))

(defn linearized-client
  "Constructs a Fauna client for the /linearized endpoint"
  [node]
  (.build (doto (FaunaClient/builder)
            (.withEndpoint (str "http://" node ":8443/linearized"))
            (.withSecret root-key))))

(defn decode
  "Takes a Fauna value and converts it to a nice Clojure value."
  [^Value x]
  (condp instance? x
    Value$ObjectV (->> (.get (Decoder/decode x (Types/hashMapOf Value)))
                       (reduce (fn [m [k v]]
                                 (assoc! m (keyword k) (decode v)))
                               (transient {}))
                       persistent!)
    Value$ArrayV  (->> (.get (Decoder/decode x (Types/arrayListOf Value)))
                       (map decode))
    Value$LongV   (.get (Decoder/decode x Long))
    (do (info "Don't know how to decode" (class x) x)
        x)))

; TODO: make this return a map?
(defn query
  "Performs a query on a connection, and returns results"
  [conn e]
  (let [r (.. conn (query (q/expr e)) (get))]
    (info "Response" (pr-str (decode r)))
    r))

(defn queryGet
  "Like query, but fetches a particular field from the results"
  [conn e field]
  (.get (query conn (q/expr e)) field))

(defn query-all
  "Performs a query for an expression. Paginates expression, performs query,
  and returns a lazy sequence of the :data from each page of results."
  ([conn expr]
   (query-all conn (q/expr expr) q/null))
  ([conn expr after]
   (lazy-seq
     (let [res (query conn (q/paginate expr after))
           data (:data (decode res))
           after (.at res (into-array String ["after"]))]
       (if (= after q/null)
         data
         (concat data (query-all conn expr after)))))))
