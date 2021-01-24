(ns jecci.common.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jecci.interface.db :as idb]
            [jepsen.db :as db]))

; implementation of jepsen.db/DB
(def db
  (if-let [_db idb/db] _db (throw (Exception. "MUST Implement db to Run jepsen!!!!"))))
