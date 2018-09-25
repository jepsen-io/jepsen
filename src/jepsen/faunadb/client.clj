(ns jepsen.faunadb.client
  "A clojure client for FaunaDB"
  (:import com.faunadb.client.FaunaClient)
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:import com.faunadb.client.types.Value)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer :all]
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

; TODO: make this return a map?
(defn query
  "Performs a query on a connection, and returns results"
  [conn expr]
  (.. conn (query expr) (get)))

(defn queryGet
  "Like query, but fetches a particular field from the results"
  [conn expr field]
  (.get (query conn expr) field))

(defn queryGetAll
  ; TODO: Rewrite as lazy seq?
  ([conn expr field] (queryGetAll conn expr field []))
  ([conn expr field results] (queryGetAll conn expr field results q/Null))
  ([conn expr field results after]
   (let [res (query conn (q/Paginate expr after))
        data (.get res field)
        after (. res (at (into-array String ["after"])))
        ret (conj results data)]
     (if (= after q/Null)
       ret
       ; Recursive query
       (queryGetAll conn expr field ret after)))))
