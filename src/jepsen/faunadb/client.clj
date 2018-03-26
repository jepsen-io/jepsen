(ns jepsen.faunadb.client
  "A clojure client for FaunaDB"
  (:import com.faunadb.client.FaunaClient)
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:require [clojure.string :as str]))

(def root-key
  "Administrative key for the FaunaDB cluster."
  "secret")

(defn client
  [node]
  (.build (doto (FaunaClient/builder)
            (.withEndpoint (str/join ["http://" node ":8443"]))
            (.withSecret root-key))))

(defn query
  [conn expr]
  (.. conn (query expr) (get)))

(defn queryGet
  [conn expr field]
  (.get (query conn expr) field))

(def LongField
  (Field/as Codec/LONG))
