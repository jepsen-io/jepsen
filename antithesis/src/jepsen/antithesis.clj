(ns jepsen.antithesis
  "Provides support for running Jepsen tests in Antithesis. Provides an RNG,
  lifecycle hooks, and assertions.

  ## Randomness

  You should wrap your entire program in `(with-rng ...)`. This does nothing in
  ordinary environments, but in Antithesis, it replaces the jepsen.random RNG
  with one powered by Antithesis.

  ## Lifecycle

  Call `setup-complete!` once the test is ready to begin. Call `event!` to
  signal interesting things have happened.

  ## Assertions

  Assertions begin with `assert-`, and take an expression, a message, and data
  to include if the assertion fails. For instance:

    (assert-always! (not (db-corrupted?)) \"DB corrupted\" {:db \"foo\"})"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info]]
            [jepsen.random :as rand])
  (:import (com.antithesis.sdk Assert
                               Lifecycle)
           (com.fasterxml.jackson.databind ObjectMapper)
           (com.fasterxml.jackson.databind.node ArrayNode
                                                ObjectNode)
           (java.io Writer)
           (java.util.random RandomGenerator
                             RandomGenerator$SplittableGenerator)
           (jepsen.antithesis Random)))

(def ^ObjectMapper om (ObjectMapper.))

(defprotocol ToJsonNode
  (->json-node [x] "Coerces x to a Jackson JsonNode."))

(extend-protocol ToJsonNode
  clojure.lang.IPersistentMap
  (->json-node [x]
    (reduce (fn [^ObjectNode n, [k v]]
              ; my kingdom for a JSON with non-string keys
              (.put n
                    (if (instance? clojure.lang.Named k)
                      (name k)
                      (str k))
                    (->json-node v))
              n)
            (.createObjectNode om)
            x))

  clojure.lang.IPersistentSet
  (->json-node [x]
    (reduce (fn [^ArrayNode a, e]
              (.add a (->json-node e)))
            (.createArrayNode om)
            x))

  clojure.lang.Sequential
  (->json-node [x]
    (reduce (fn [^ArrayNode a, e]
              (.add a (->json-node e)))
            (.createArrayNode om)
            x))

  clojure.lang.Keyword
  (->json-node [x]
    (name x))

  clojure.lang.BigInt
  (->json-node [x]
    ; Weirdly Jackson takes bigdecimals but not bigintegers
    (bigdec x))

  java.lang.Number
  (->json-node [x] x)

  java.lang.String
  (->json-node [x] x)

  java.lang.Boolean
  (->json-node [x] x)

  nil
  (->json-node [x] nil))

(let [d (delay (System/getenv "ANTITHESIS_OUTPUT_DIR"))]
  (defn dir
    "The Antithesis SDK directory, if present, or nil."
    []
    @d)

  (defn log-file
    "The Antithesis log file we write JSON to, or nil."
    []
    (when-let [d @d]
      (str d "/sdk.jsonl"))))

(defn antithesis?
  "Are we running in an Antithesis environment?"
  []
  (boolean (dir)))

;; Randomness

(defmacro with-rng
  "When running in an Antithesis environment, replaces Jepsen's random source
  with an Antithesis-controlled source. You should wrap your top-level program
  in this."
  [& body]
  `(rand/with-rng (if (antithesis?)
                    (Random.)
                    rand/rng)
     ~@body))

;; Lifecycle

(defn setup-complete!
  "Logs that we've started up. Only emits once per JVM run. Optionally takes a
  JSON-coerceable structure with details."
  ([]
   (setup-complete! nil))
  ([details]
   (Lifecycle/setupComplete (->json-node details))))

(defn event!
  "Logs that we've reached a specific event. Takes a string name and an
  optional JSON-coerceable details map."
  ([name]
   (event! name nil))
  ([name details]
   (Lifecycle/sendEvent name (->json-node details))))

;; Assertions

(defmacro assert-always
  "Asserts that expr is true every time, and that it's called at least once.
  Takes a map of data which is serialized to JSON."
  [expr message data]
  `(Assert/always (boolean ~expr)
                  ~message
                  (->json-node ~data)))

(defmacro assert-always-or-unreachable
  "Asserts that expr is true every time. Passes even if the assertion never
  triggers."
  [expr message data]
  `(Assert/alwaysOrUnreachable (boolean ~expr)
                               ~message
                               (->json-node ~data)))

(defmacro assert-sometimes
  "Asserts that expr is true at least once, and that this assertion is reached."
  [expr message data]
  `(Assert/sometimes (boolean ~expr)
                     ~message
                     (->json-node ~data)))

(defmacro assert-unreachable
  "Asserts that this line of code is never reached."
  [message data]
  `(Assert/unreachable ~message ~data))

(defmacro assert-reachable
  "Asserts that this line of code is reached at least once."
  [message data]
  `(Assert/reachable ~message ~data))
