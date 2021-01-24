(ns jecci.settings
  (:require [clojure.tools.logging :refer [info warn]]))

(def system "postgres")

; Followings are for utils/dbms/sql.clj
; It is a pretty nasty implementation here...
; Since they are settings for jdbc, which means
; they do not fit for all distributed systems, like etcd
; 
; Should (hopefully) change in the future!
(def txn-timeout     5000)
(def connect-timeout 10000)
(def socket-timeout  10000)
(def open-timeout
  "How long will we wait for an open call by default"
  5000)

(defn conn-spec
  "jdbc connection spec for a node."
  [node]
  {:dbtype          "postgresql"
   :dbname          "postgres"
   :user            "jecci"
   :password        "123456"
   :host            (name node)
   :connectTimeout  connect-timeout
   :socketTimeout   socket-timeout})

