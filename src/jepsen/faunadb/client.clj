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
                                     Value$DoubleV
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
      Value$ObjectV (->> (.get (Decoder/decode x (Types/hashMapOf Value)))
                         (reduce (fn [m [k v]]
                                   (assoc! m (keyword k) (decode v)))
                                 (transient {}))
                         (persistent!))

      Value$RefV    (Ref. (decode (.orElse (.getDatabase x) nil))
                          (decode (.orElse (.getClazz x) nil))
                          (.getId x))
      Value$ArrayV  (->> (.get (Decoder/decode x (Types/arrayListOf Value)))
                         (map decode))
      Value$LongV    (.get (Decoder/decode x Long))
      Value$DoubleV  (.get (Decoder/decode x Double))
      Value$BooleanV (.get (Decoder/decode x Boolean))
      Value$StringV  (.get (Decoder/decode x String))
      (do (info "Don't know how to decode" (class x) x)
          x))))

(defn query*
  "Raw version of query; doesn't decode results."
  [conn e]
  (try
    ; Basically every exception thrown by the client is
    ; concurrentexecutionexception. This makes error handling awkward because
    ; you can't catch error types any more. We can work around that by throwing
    ; the cause, but then the stacktrace is just from the internal netty
    ; executor and tells us nothing about what code actually hit the exception.
    ; I don't have a good solution to this (pattern-matching catch expressions
    ; which can examine causes?).
    ;
    ; So... a compromise: for Jepsen's purposes we don't care much about Netty
    ; internals, so we're going to *replace* the original cause stacktrace with
    ; the CEE's stacktrace, which at least tells you where in Jepsen's code
    ; things went wrong.
    (.. conn (query (q/expr e)) (get))
    (catch java.util.concurrent.ExecutionException e
      (let [cause (.getCause e)]
        (.setStackTrace cause (.getStackTrace e))
        (throw cause)))))

(defn query
  "Performs a query on a connection, and returns results."
  [conn e]
  (decode (query* conn e)))

(defn now
  "Queries FaunaDB for the current time."
  [conn]
  (query conn (q/time "now")))

(defn query-all
  "Performs a query for an expression. Paginates expression, performs query,
  and returns a lazy sequence of the :data from each page of results."
  ([conn expr]
   (query-all conn (q/expr expr) q/null))
  ([conn expr after]
   (info :query-all-after after)
   (lazy-seq
     (let [res   (query* conn (q/paginate expr after))
           data  (:data (decode res))
           after (.at res (into-array String ["after"]))]
       (if (= after q/null)
         data
         (concat data (query-all conn expr after)))))))
