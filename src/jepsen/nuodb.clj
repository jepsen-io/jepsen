(ns jepsen.nuodb
  (:use [clojure.set :only [union difference]]
        [korma.core :exclude [union]]
        [korma.db :only [create-db with-db transaction]]
        jepsen.set-app)
  (:import (com.nuodb.jdbc Driver))
  (:require [clojure.string :as string]
            [cheshire.core :as json]))

(defn connect [host]
  (Class/forName "com.nuodb.jdbc.Driver")
  (create-db {:classname "com.nuodb.jdbc.Driver"
              :subprotocol "com.nuodb"
              :subname (str "//" host "/jepsen?")
              :user "jepsen"
              :password "jepsen"
              :schema "system"}))

(defmacro cas [& body]
  `(loop []
     (let [result# (try
                     ~@body
                     (catch RuntimeException e#
                       (log "runtime ex" e#)
                       (if-let [cause# (.getCause e#)]
                         (throw cause#)
                         (throw e#)))
                     (catch java.sql.SQLException e#
                       (if (or (re-find #"update conflict" (.getMessage e#))
                               (re-find #"pending update rejected"
                                        (.getMessage e#)))
                         ::retry
                         (throw e#))))]
     (if (= ::retry result#)
       (do
         (log :retry)
         (recur))
       result#))))

(def db-lock (Object.))

(defn nuodb-app
  [opts]
  (let [table   (get opts :coll "SET_APP")
        db      (connect (:host opts))]

    (reify SetApp
      (setup [app]
             (locking db-lock
               (teardown app)
               (with-db db
                        (exec-raw "CREATE TABLE SET_APP (
                                  ID INTEGER GENERATED ALWAYS AS IDENTITY,
                                  JSON TEXT NOT NULL)")
                        (insert table (values {:JSON "[]"}))
                        (assert (-> table
                                  (select (aggregate (count :ID) :count))
                                  first
                                  :count
                                  (= 1))))))

      (add [app element]
        (let [write (future
                      (cas
                        (with-db db
                          (transaction
                            (let [numbers (-> table
                                              (select (limit 1))
                                              first
                                              :JSON
                                              json/parse-string
                                              (conj element)
                                              json/generate-string)]
                              (update table (set-fields {:JSON numbers})))))))
              res (deref write 5000 ::timeout)]
              (when (= res ::timeout)
                (future-cancel write)
                (throw (RuntimeException. "timeout")))
              res))

      (results [app]
        (with-db db
          (-> table
              (select (limit 1))
              first
              :JSON
              json/parse-string)))

      (teardown [app]
        (locking db-lock
          (with-db db
            (exec-raw "DROP TABLE IF EXISTS SET_APP CASCADE")))))))
