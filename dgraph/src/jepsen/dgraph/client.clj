(ns jepsen.dgraph.client
  "Clojure wrapper for the Java DGraph client library."
  (:require [clojure.string :as str]
            [wall.hack]
            [cheshire.core :as json])
  (:import (com.google.protobuf ByteString)
           (io.grpc ManagedChannel
                    ManagedChannelBuilder)
           (io.dgraph DgraphGrpc
                      DgraphClient
                      DgraphClient$Transaction
                      DgraphProto$Assigned
                      DgraphProto$Mutation
                      DgraphProto$Response
                      DgraphProto$Operation)))

(def default-port "Default dgraph alpha GRPC port" 9080)

(def deadline "Timeout in seconds" 5)

(defn open
  "Creates a new DgraphClient for the given node."
  ([node]
   (open node default-port))
  ([node port]
   (let [channel (.. (ManagedChannelBuilder/forAddress node port)
                     (usePlaintext true)
                     (build))
         blocking-stub (DgraphGrpc/newBlockingStub channel)]
     (DgraphClient. [blocking-stub] deadline))))

(defn close!
  "Closes a client. Close is asynchronous; resources may be freed some time
  after calling (close! client)."
  [client]
  (doseq [c (wall.hack/field DgraphClient :clients client)]
    (.. c getChannel shutdown)))

(defmacro with-txn
  "Takes a vector of a symbol and a client. Opens a transaction on the client,
  binds it to that symbol, and evaluates body. Calls commit at the end of
  the body, or discards the transaction if an exception is thrown. Ex:

      (with-txn [t my-client]
        (mutate! t ...)
        (mutate! t ...))"
  [[txn-sym client] & body]
  `(let [~txn-sym (.newTransaction ^DgraphClient ~client)]
     (try
       ~@body
       (.commit ~txn-sym)
       (finally
         (.discard ~txn-sym)))))

(defn str->byte-string
  "Converts a string to a protobuf bytestring."
  [s]
  (ByteString/copyFromUtf8 s))

(defn alter-schema!
  "Takes a schema string (or any number of strings) and applies that alteration
  to dgraph."
  [^DgraphClient client & schemata]
  (.alter client (.. (DgraphProto$Operation/newBuilder)
                     (setSchema (str/join "\n" schemata))
                     build)))

(defn ^DgraphProto$Assigned mutate!
  "Takes a mutation object and applies it to a transaction. Returns an
  Assigned."
  [^DgraphClient$Transaction txn mut]
  (.mutate txn (.. (DgraphProto$Mutation/newBuilder)
                   (setSetJson (str->byte-string (json/generate-string mut)))
                   build)))
