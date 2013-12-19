(ns jepsen.nuodb
  (:use [clojure.set :only [union difference]]
        [korma.core :exclude [union]]
        [korma.db :only [create-db with-db transaction]]
        jepsen.util
        jepsen.load
        jepsen.set-app)
  (:import (com.nuodb.jdbc Driver))
  (:require [clojure.string :as string]
            [cheshire.core :as json]))

(defn connect [host port]
  (let [fullhost (if (nil? port)
                   host
                   (str host ":" port))]
    
   (Class/forName "com.nuodb.jdbc.Driver")
   (create-db {:classname "com.nuodb.jdbc.Driver"
               :subprotocol "com.nuodb"
               :subname (str "//" fullhost "/jepsen?")
               :user "jepsen"
               :password "jepsen"
               :schema "jepsen"
			   :make-pool? true
			   :maximum-pool-size 200 ;1000 connections for the 5 host setup
			   })
   ))

(defmacro cas [& body]
  `(loop []
     (let [result# (try
                     ~@body
                     (catch java.sql.SQLException e#
                       (if (or (re-find #"update conflict" (.getMessage e#))
                               (re-find #"pending update rejected"
                                        (.getMessage e#)))
                         ::retry
                         (throw e#)))
                     (catch RuntimeException e#
                       (log "runtime ex" e#)
                       (if-let [cause# (.getCause e#)]
                         (throw cause#)
                         (throw e#))))]
     (if (= ::retry result#)
       (do
         (recur))
       result#))))

(def db-lock (Object.))

;the db-lock wasn't preventing every worker from dropping and re-adding the same table
(def ddled   (atom 0N))

(def TIMEOUT-MILLIS 65000)

;;Query strings
(defn create-table-cmd
  [opts]
  (if (= "insert" (:special opts))
    ; Use the entire table as our set representation
    "CREATE TABLE SET_APP (ID INTEGER GENERATED ALWAYS AS IDENTITY, ELEMENT INTEGER NOT NULL)"
    ; Use a CLOB to store a JSON representation of our set
    "CREATE TABLE SET_APP (ID INTEGER GENERATED ALWAYS AS IDENTITY, JSON TEXT NOT NULL)"))

(defn initial-value
  [table opts]
  (when  (not (= "insert" (:special opts)))
    (insert table (values {:JSON "[]"}))))

(defn check-setup
  [table opts]
  (if (= "insert" (:special opts))
    (assert (-> table
                (select (aggregate (count :ID) :count))
                first
                :count
                (= 0)))
    (assert (-> table
                (select (aggregate (count :ID) :count))
                first
                :count
                (= 1)))
    ))

(defmacro retry-on-disconnect [& body]
  `(loop []
     (let [result# (try
                     ~@body
                     (catch java.io.IOException e#
                       ::retry))]
       (if (= ::retry result#)
         (do
           (recur))
         result#)
       )))

(defn build-add-future
  [db table element opts]
  (if (= "insert" (:special opts))
    ;table as set
    (future
      (retry-on-disconnect
       (with-db db
         (transaction (insert table (values {:element element}))))))
    ;CLOB as json set
    (future
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
            (update table (set-fields {:JSON numbers}))
            )))))
    )
  )

(defn check-results
  [db table opts]
  (if (= "insert" (:special opts))
    (map :ELEMENT
         (with-db db (transaction (select table (fields :element)))))
    (with-db db
      (-> table
          (select (limit 1))
          first
          :JSON
          json/parse-string))
    ))

(defn nuodb-app
  [opts]
  (let [table   (get opts :coll "JEPSEN.SET_APP")
	    db      (connect (:host opts) (:port opts))]

    (reify SetApp
      (setup [app]
        (locking db-lock
		  (when (= @ddled 0)
			(swap! ddled inc)
			(teardown app)
			(with-db db
              (exec-raw "USE JEPSEN")
              (exec-raw "DROP TABLE SET_APP CASCADE IF EXISTS")
              (exec-raw (create-table-cmd opts))
              (initial-value table opts)
              (check-setup table opts)
              ))
		  ))

      (add [app element]
        (let [write (build-add-future db table element opts)
              res   (deref write TIMEOUT-MILLIS ::timeout)]
          (when (= res ::timeout)
            (future-cancel write)
            (throw (RuntimeException. "timeout")))
          ok))

      (results [app]
        (check-results db table opts))

      (teardown [app]
        (locking db-lock
          (with-db db
            (exec-raw "DROP TABLE JEPSEN.SET_APP IF EXISTS CASCADE")))))))
