(ns jepsen.antithesis
  "Provides support for running Jepsen tests in Antithesis. Provides an RNG,
  lifecycle hooks, and assertions.

  ## Randomness

  You should wrap your entire program in `(with-rng ...)`. This does nothing in
  ordinary environments, but in Antithesis, it replaces the jepsen.random RNG
  with one powered by Antithesis.

  ## Lifecycle

  Call `setup-complete!` once the test is ready to begin--for instance, at the
  end of Client/setup. Call `event!` to signal interesting things have
  happened.

  ## Assertions

  Assertions begin with `assert-`, and take an expression, a message, and data
  to include if the assertion fails. For instance:

    (assert-always! (not (db-corrupted?)) \"DB corrupted\" {:db \"foo\"})

  ## Wrappers

  Wrap your test map in `(a/test test-map)`. This wraps both the client and
  checkers with Antithesis instrumentation. You can also do this selectively
  using `checker` and `client`."
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [db :as db]
                    [generator :as gen]
                    [os :as os]
                    [random :as rand]])
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

(defn replacement-double-weighted-index
  "Antithesis replacement for jepsen.random/double-weighted-index. Takes a
  double array, and picks a random index into it."
  (^long [^doubles weights]
         (replacement-double-weighted-index 0.0 weights))
  (^long [^double total-weight ^doubles weights]
         (.randomChoice ^Random rand/rng (range (alength weights)))))

(def choice-cardinality
  "When selecting long values, we consider something an Antithesis \"choice\"
  if it asks for at most this many elements."
  16)

(defn replacement-long
  "Antithesis replacement for `jepsen.random/long`. Mostly equivalent to the
  original, but when there are less than `choice-cardinality` options, hints to
  Antithesis that we're making a specific choice."
  (^long [] (.nextLong rand/rng))
  (^long [^long upper]
         (if (< 0 upper choice-cardinality)
           (.randomChoice ^Random rand/rng (range upper))
           (.nextLong rand/rng upper)))
  (^long [^long lower, ^long upper]
         (if (< 0 (- upper lower) choice-cardinality)
           (.randomChoice ^Random rand/rng (range lower upper))
           (.nextLong rand/rng lower upper))))

(defn replacement-bool
  "Antithesis replacement for `jepsen.random/bool`. Uses Antithesis choices
  when probabilities are closer to chance than choice-cardinality."
  ([] (.nextBoolean ^Random rand/rng))
  ([^double p]
   (if (< (/ choice-cardinality)
          p
          (- 1 (/ choice-cardinality)))
     (.nextBoolean ^Random rand/rng)
     (< (rand/double) p))))

(defmacro with-rng
  "When running in an Antithesis environment, replaces Jepsen's random source
  with an Antithesis-controlled source. You should wrap your top-level program
  in this."
  [& body]
  `(rand/with-rng (if (antithesis?)
                    (Random.)
                    rand/rng)
     (with-redefs [jepsen.random/double-weighted-index
                   (if (antithesis?)
                     replacement-double-weighted-index
                     rand/double-weighted-index)

                   jepsen.random/long
                   (if (antithesis?)
                     replacement-long
                     rand/long)

                   jepsen.random/bool
                   (if (antithesis?)
                     replacement-bool
                     rand/bool)]
       ~@body)))

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
  `(Assert/unreachable ~message (->json-node ~data)))

(defmacro assert-reachable
  "Asserts that this line of code is reached at least once."
  [message data]
  `(Assert/reachable ~message (->json-node ~data)))

;; Client

(defrecord Client [client]
  client/Client
  (open! [this test node]
    (Client. (client/open! client test node)))

  (setup! [this test]
    (let [r (client/setup! client test)]
      (setup-complete!)
      r))

  (invoke! [this test op]
    ; Every Jepsen run should do at *least* one invocation. We emit one of
    ; these for each kind of :f the client does. TODO: I'm not sure if we
    ; should provide details or not.
    ;
    ; TODO: is it acceptable for us to runtime generate the name for this
    ; assertion? My guess is that whatever magic instrumentation Antithesis'
    ; SDK is doing might expect compile-time constants here, but the SDK docs
    ; don't say.
    (assert-reachable (str "invoke "  (pr-str (:f op))) {})
    (let [op' (client/invoke! client test op)]
      ; We'd like at least one invocation to succeed.
      ;
      ; TODO: is this OK? It feels like Antithesis could trivially generate a
      ; counterexample by just making the system unavailable, and that's not
      ; actually a bug. What SHOULD I be doing here?
      (assert-sometimes (identical? :ok (:type op'))
                        (str "ok " (pr-str (:f op)))
                        ; We'll at least log the type it came back with?
                        (select-keys op' [:type]))
      op'))

  (teardown! [this test]
    (client/teardown! client test))

  (close! [this test]
    (client/close! client test)))

(defn client
  "Wraps a Jepsen Client in one which performs some simple Antithesis
  instrumentation. Calls `setup-complete!` after the client's setup is done,
  and issues a pair of assertions for every operation--once on invocation, and
  once on completion."
  [client]
  (if (antithesis?)
    (Client. client)
    client))

; We build these as protocols so that they can be extended *both* over the
; built-in checkers, and also by new checkers.
(defprotocol Checker
  "This protocol marks checker types which perform their own Antithesis
  assertions. The `checker+` wrapper skips over any checkers which satisfy this
  protocol, as well as their children.")

(extend-protocol Checker
  ; The Stats checker will fail on histories with no :ok operations of some
  ; type, which Antithesis will sometimes generate.
  jepsen.checker.Stats)

(defrecord AlwaysChecker [^String name checker]
  Checker
  checker/Checker
  (check [this test history opts]
    (let [r (checker/check checker test history opts)]
      (assert-always (true? (:valid? r)) name r)
      r)))

(defn checker
  "Wraps a Jepsen Checker in one which asserts the results of the underlying
  checker are always valid. Takes a string name, which defaults to \"checker\";
  this is used for the Antithesis assertion."
  ([checker-]
   (checker "checker" checker-))
  ([name checker]
   (if (antithesis?)
     (AlwaysChecker. name checker)
     checker)))

(defn checker+
  "Rewrites a Jepsen Checker, wrapping every Checker in its own Antithesis
  Checker, which asserts validity of that specific checker. Each checker gets a
  name derived from the path into that data structure it took to get there.

  Ignores any object which satisfies the Checker protocol. You can use this to
  flag checkers you don't want to add assertions for, or which implement their
  own assertions."
  ([c]
   (checker+ ["checker"] c))
  ([path c]
   ; (prn :rewrite path c)
   (let [; We often rewrite singleton maps; in these cases, don't bother
         ; propagating paths. For example, a Compose has only a single key
         ; :checkers, and we don't need to generate names like "checker
         ; :checkers :cat"; we can just do "checker :cat".
         branch? (and (coll? c) (< 1 (count c)))
         ; Rewrite child forms
         c' (cond ; Already aware of Antithesis checking; don't explore any
                  ; deeper
                  (satisfies? Checker c)
                  c

                  ; For maps and records, rewrite each value, using the key as
                  ; our path
                  (map? c)
                  (reduce (fn [c [k v]]
                            (assoc c k (checker+
                                         (if branch?
                                           (conj path (pr-str k))
                                           path)
                                         v)))
                          c
                          c)

                  ; For sets, rewrite without changing path
                  (set? c)
                  (reduce (fn [c v]
                            (conj c (checker+ path v)))
                          (empty c)
                          c)

                  ; For sequential collections, rewrite without changing path
                  (sequential? c)
                  (reduce (fn [c v]
                            (conj c (checker+ path v)))
                          (empty c)
                          c)

                  ; Everything else bottoms out in itself
                  true
                  c)]
     ; Now, consider the rewritten thing.
     (cond ; Our Checkers handle their own assertions.
           (satisfies? Checker c')
           c'

           ; If it's a Jepsen checker, wrap it, using the current path as our
           ; name.
           (satisfies? checker/Checker c')
           (checker (str/join " " path) c')

           ; Otherwise, return unchanged.
           true
           c'))))

(defn test
  "Wraps a Jepsen test in Antithesis wrappers. Lifts the client and checker
  both. If (:antithesis? test) is truthy, also:

  1. Replaces the OS with a no-op
  2. Repaces the DB with a no-op
  3. Replaces the SSH system with a stub."
  [test]
  (cond-> test
      true (update :client client)
      true (update :checker checker+)
      (:antithesis? test) (assoc :os os/noop
                                 :db db/noop
                                 :ssh {:dummy? true})))
