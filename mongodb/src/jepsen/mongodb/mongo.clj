(ns jepsen.mongodb.mongo
  "MongoDB java driver adapter"
  (:refer-clojure :exclude [])
  (:require [clojure.core :as c]
            [clojure.tools.logging :refer :all]
            [jepsen.util :refer [timeout with-retry]])
  (:import (clojure.lang ExceptionInfo)
           (java.util ArrayList
                      List)
           (org.bson Document)
           (com.mongodb MongoClient
                        MongoClientOptions
                        ReadConcern
                        ReadPreference
                        ServerAddress
                        WriteConcern)
           (com.mongodb.client MongoCollection
                               MongoDatabase
                               MongoIterable)
           (com.mongodb.client.model Filters
                                     FindOneAndUpdateOptions
                                     ReturnDocument
                                     Sorts
                                     Updates
                                     UpdateOptions
                                     )
           (com.mongodb.client.result UpdateResult)))

(defn ^MongoClientOptions default-client-options
  "MongoDB client options."
  []
  (.build
    (doto (MongoClientOptions/builder)
      (.maxWaitTime     20000)
      (.connectTimeout  5000)
      (.socketTimeout   10000))))

(defn client
  "Creates a new Mongo client."
  [node]
  (MongoClient. (name node) (default-client-options)))

(defn cluster-client
  "Returns a mongoDB connection for all nodes in a test."
  [test]
  (MongoClient. (->> test :nodes (map #(ServerAddress. (name %))))
                (default-client-options)))

(defn ^MongoDatabase db
  "Gets a Mongo database from a client."
  [^MongoClient client db]
  (.getDatabase client db))

(defn ^MongoCollection collection
  "Gets a Mongo collection from a DB."
  [^MongoDatabase db collection-name]
  (.getCollection db collection-name))

(def read-concerns
  "A map of read concern keywords to java driver constants."
  {:majority ReadConcern/MAJORITY
   :local    ReadConcern/LOCAL
   :default  ReadConcern/DEFAULT})

(def write-concerns
  "A map of write concern keywords to java driver constants."
  {:acknowledged    WriteConcern/ACKNOWLEDGED
   :journaled       WriteConcern/JOURNALED
   :unacknowledged  WriteConcern/UNACKNOWLEDGED
   :w1              WriteConcern/W1
   :w2              WriteConcern/W2
   :w3              WriteConcern/W3
   :majority        WriteConcern/MAJORITY})

(defn ^MongoCollection with-read-concern
  "Returns a copy of the given collection, using the given read concern
  keyword."
  [^MongoCollection coll read-concern]
  (let [read-concern (c/get read-concerns read-concern)]
    (assert read-concern)
    (.withReadConcern coll read-concern)))

(defn ^MongoCollection with-write-concern
  "Returns a copy of the given collection, using the given write concern
  keyword."
  [^MongoCollection coll write-concern]
  (let [write-concern (c/get write-concerns write-concern)]
    (assert write-concern)
    (.withWriteConcern coll write-concern)))

(defn document
  "Creates a Mongo document from a map."
  [m]
  (reduce (fn [doc [k v]]
            (.append doc
                     (if (keyword? k) (name k) k)
                     (cond
                       (keyword?  v) (name v)
                       (map?      v) (document v)
                       (coll?     v) (ArrayList. (map document v))
                       true          v))
            doc)
          (Document.)
          m))

(defn document->map
  "Converts a document back into a map."
  [^Document doc]
  (when-not (nil? doc)
    (->> doc
         .entrySet
         (reduce (fn [m [k v]]
                   (assoc m
                          (keyword k)
                          (cond (instance? Document v) (document->map v)
                                (instance? List v)     (map document->map v)
                                true                   v)))
                 {}))))

(defn create-collection!
  "Create a collection in a database."
  [^MongoDatabase db collection-name]
  (.createCollection db collection-name))

(defn drop-collection!
  "Drops a collection."
  [^MongoCollection collection]
  (.dropCollection collection))

(defn parse-result
  "Parses a command's result into a Clojure data structure."
  [doc]
  (document->map doc))

(defn iterable-seq
  "Turns a MongoIterable into a seq."
  [^MongoIterable i]
  (-> i .iterator iterator-seq))

(defn run-command!
  "Runs an arbitrary command on a database. Command is a flat list of kv pairs,
  with the first pair being the command name, which will be transformed into a
  document. Includes a hardcoded 10 second timeout."
  [^MongoDatabase db & command]
  (->> command
       (partition 2)
       document
       (.runCommand db)
       parse-result))
;       (timeout 10000
;                (throw (ex-info "timeout" {:db (.getName db) :cmd command})))))

(defn admin-command!
  "Runs a command on the admin database."
  [client & command]
  (apply run-command! (db client "admin") command))

(defn find-one
  "Find a document by ID."
  [^MongoCollection coll id]
  (-> coll
      (.find (Filters/eq "_id" id))
      .first
      document->map))

(defn read-with-find-and-modify
  "Perform a read of a document by ID with findAndModify."
  [^MongoCollection coll id]
  (-> coll
      (with-write-concern :majority)
      (.findOneAndUpdate (Filters/eq "_id" id)
                         (Updates/inc "_dummy_field" 1)
                         (-> (FindOneAndUpdateOptions.)
                             (.returnDocument ReturnDocument/AFTER)))
      document->map))

(defn update-result->map
  "Converts an update result to a clojure map."
  [^UpdateResult r]
  {:matched-count  (.getMatchedCount r)
   :modified-count (when (.isModifiedCountAvailable r)
                     (.getModifiedCount r))
   :upserted-id    (.getUpsertedId r)
   :acknowledged?  (.wasAcknowledged r)})

(defn replace!
  "Replace a document by a document's :_id."
  [^MongoCollection coll doc]
  (-> coll
      (.replaceOne (Filters/eq "_id" (:_id doc))
                   (document doc))
      update-result->map))

(defn cas!
  "Atomically replace doc with doc' in coll."
  [^MongoCollection coll doc doc']
  (-> coll
      (.replaceOne (document doc)
                   (document doc'))
      update-result->map))

(defn upsert!
  "Ensures the existence of the given document, a map with at minimum an :_id
  key."
  [^MongoCollection coll doc]
  (assert (:_id doc))
  (with-retry []
    (-> coll
        (.replaceOne (Filters/eq "_id" (:_id doc))
                     (document doc)
                     (.upsert (UpdateOptions.) true))
        update-result->map)
    (catch com.mongodb.MongoWriteException e
      ; This is probably
      ; https://jira.mongodb.org/browse/SERVER-14322; we back off randomly
      ; and retry.
      (if (= 11000 (.getCode e))
        (do (info "Retrying duplicate key collision")
            (Thread/sleep (rand-int 100))
            (retry))
        (throw e)))))
