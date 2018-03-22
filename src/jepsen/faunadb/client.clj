(ns jepsen.faunadb.client
  "A clojure client for FaunaDB"
  (:import com.faunadb.client.FaunaClient)
  (:require [clojure.string :as str]))

(defn client
  [node root-key]
  (.build (doto (FaunaClient/builder)
            (.withEndpoint (str/join ["http://" node ":8443"]))
            (.withSecret root-key))))

(defn query
  [conn expr]
  (.toString (.. conn (query expr) (get))))
