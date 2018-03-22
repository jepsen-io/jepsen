(ns jepsen.faunadb.client
  "A clojure client for FaunaDB"
  (:import com.faunadb.client.FaunaClient)
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:require [clojure.string :as str]))

(defn client
  [node root-key]
  (.build (doto (FaunaClient/builder)
            (.withEndpoint (str/join ["http://" node ":8443"]))
            (.withSecret root-key))))

(defn query
  [conn expr]
  (.. conn (query expr) (get)))

(defn get
  [conn expr field]
  (.get (query conn expr) field))

(def LongField
  (Field/as Codec/LONG))
