(ns jepsen.faunadb.client
  "A clojure client for FaunaDB"
  (:import (com.faunadb.client FaunaClient)
           (com.faunadb.client.types Codec
                                     Decoder
                                     Field
                                     Value
                                     Value$ObjectV
                                     Value$ArrayV
                                     Value$RefV
                                     Value$LongV
                                     Value$StringV
                                     Value$BooleanV
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

(defrecord Ref [db class id])

(defn decode
  "Takes a Fauna value and converts it to a nice Clojure value."
  [^Value x]
  (when x
    (condp instance? x
      Value$ObjectV
      (let [m (->> (.get (Decoder/decode x (Types/hashMapOf Value)))
                   (reduce (fn [m [k v]]
                             (assoc! m (keyword k) (decode v)))
                           (transient {})))
            ; For some reason the after key isn't a part of map decoding, at
            ; least for single-page pagination. I think they might just leave
            ; it off, but... if you're experimenting with longer pagination
            ; later, try this. It's what Fauna did for pagination originally.
            m (if-let [a (.at x (into-array String ["after"]))]
                (assoc! m :after a)
                m)]
        (persistent! m))

      Value$RefV    (Ref. (decode (.orNull (.getDatabase x)))
                          (decode (.orNull (.getClazz x)))
                          (.getId x))
      Value$ArrayV  (->> (.get (Decoder/decode x (Types/arrayListOf Value)))
                         (map decode))
      Value$LongV    (.get (Decoder/decode x Long))
      Value$BooleanV (.get (Decoder/decode x Boolean))
      Value$StringV  (.get (Decoder/decode x String))
      (do (info "Don't know how to decode" (class x) x)
          x))))

; TODO: make this return a map?
(defn query
  "Performs a query on a connection, and returns results"
  [conn e]
  (decode (.. conn (query (q/expr e)) (get))))

(defn queryGet
  "Like query, but fetches a particular field from the results"
  [conn e field]
  (let [r (query conn e)]
    (info :result (with-out-str (pprint r)))
    r))

(defn query-all
  "Performs a query for an expression. Paginates expression, performs query,
  and returns a lazy sequence of the :data from each page of results."
  ([conn expr]
   (query-all conn (q/expr expr) q/null))
  ([conn expr after]
   (lazy-seq
     (let [res   (query conn (q/paginate expr after))
           data  (:data res)
           after (:after res)]
       (if (= after q/null)
         data
         (concat data (query-all conn expr after)))))))


