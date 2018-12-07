(ns jepsen.faunadb.query
  "A nice clojure interface for FaunaDB-JVM"
  (:refer-clojure :only [defn defmacro])
  (:require [clojure.core :as c]
            wall.hack
            [clojure.tools.logging :refer [info warn]])
  (:import (com.faunadb.client.query Language
                                     Expr)
           (com.faunadb.client.query Fn$Unescaped
                                     Fn$UnescapedObject
                                     Fn$UnescapedArray)
           (com.faunadb.client.types Value$TimeV)
           (java.time Instant)))

(def null
  (Language/Null))

(defn expr
  "Convenience method for constructing query expressions. Turns several Clojure
  primitives and collections into corresponding Language types, so instead of:

      (Arr (Obj \"field\" (Arr (v \"data\") (v \"foo\"))))

  You can write

      [{:field [\"data\" \"foo\"]}].

  - Fauna Language Exprs are passed through unchanged.
  - Clojure Maps become Fauna Objects (keys may be strings or keywords)
  - Sequentials become Arrays
  - Strings, Numbers, etc become Values
  - Nil becomes Null
  - Instants become TimeVs
  "
  [x]
  (c/cond
    (c/instance? Expr x)    x
    (c/instance? Instant x) (Value$TimeV. x)
    (c/map? x)          (c/->> x
                               (c/map (c/fn [[k v]] [(c/name k) (expr v)]))
                               (c/into {})
                               (Language/Obj))
    (c/sequential? x)   (Language/Arr (c/mapv expr x))
    (c/string? x)       (Language/Value x)
    (c/number? x)       (Language/Value x)
    (c/true? x)         (Language/Value x)
    (c/false? x)        (Language/Value x)
    (c/nil? x)          null
    true                x))

; Basic types

(defn value
  [value]
  (Language/Value value))

; var is hardcoded into the language reader, can't be THAT cute here :(((
(defn variable
  "Constructs a variable name"
  [var]
  (Language/Var var))

(defn class
  "Takes a string name"
  [class-name]
  (if (c/instance? Expr class-name)
    ; Pass through classes as-is
    class-name
    (Language/Class (expr class-name))))

(defn ref
  "Takes a string class name and a string id in that class; constructs a
  ClassRef."
  [class-name id]
  (Language/Ref (class class-name) (expr id)))

(defn index
  "Takes a string and constructs an index ref."
  [index-name]
  (Language/Index (expr index-name)))

(defn array
  [& vs]
  (Language/Arr (c/mapv expr vs)))

; Syntax

(defn do
  "With 0 args, returns Null. With 1 arg, returns arg. With multiple args,
  constructs a Do."
  ([] null)
  ([x] (expr x))
  ([x & xs]
   (Language/Do (c/mapv expr (c/cons x xs)))))

(defn do*
  "Like do, but takes a seq of exprs, rather than varargs."
  [exprs]
  (c/condp c/= (c/count exprs)
    0 null
    1 (expr (c/first exprs))
    (Language/Do (c/mapv expr exprs))))

(defmacro fn
  "Macro for lambda creation. Takes an arg vector [x], followed by a body.
  Wraps body in an implicit Do, if multiple body forms are provided. Instances
  of the binding vector's symbols in body are replaced by vars, e.g. `x`
  becomes (Var x)."
  [args & body]
  (c/let [; Construct an argument vector like (array "x")
          fauna-bindings `(array ~@(c/map c/name args))
          ; Construct a binding vector like [x (variable "x")]
          let-bindings (c/mapcat (c/fn [arg]
                               [arg `(variable ~(c/name arg))])
                           args)]
    `(Language/Lambda
       ~fauna-bindings
       (c/let [~@let-bindings]
         (jepsen.faunadb.query/do ~@body)))))

(defmacro let
  "Takes a binding expression, like Clojure, and a body. Creates a Let
  expression with `bindings`. Within `body`, binding symbols refer to Vars, as
  you'd expect. Body is evaluated in an implicit do."
  [bindings & body]
  (c/let [bindings (c/partition 2 bindings)
          ; Each binding is now a pair like [x (q/get ...)]. We're going to
          ; emit a faunadb Let binding, but with instances of `x` replaced by
          ; `(variable "x")`, both in our body, AND in all later RHS bindings.
          ;
          ; To do this lexically, we're going to construct a series of Clojure
          ; let bindings (flattened, e.g. a x b y), which we'll wrap around the
          ; body. We'll also wrap RHS expressions in the Fauna bindings with
          ; let bindings *up* to that point.
          let-bindings (c/mapcat (c/fn [[sym _]]
                                   [sym `(variable ~(c/name sym))])
                                 bindings)

          ; Fauna Let takes a map of string variable names to Exprs. Note that
          ; in each RHS, we're going to wrap the expr in a clojure let binding
          ; which replaces instances of lexical variables with the appropriate
          ; `(variable x)` form.
          fauna-bindings
          (c/->> bindings
                 (c/map-indexed (c/fn [i [sym e]]
                                  ; Take the variable bindings that have been
                                  ; established at this point
                                  (c/let [bs (c/take (c/* 2 i) let-bindings)]
                                    ; And construct our pair of string to expr
                                    [(c/name sym)
                                     ; Make sure the RHS observes past bindings
                                     `(c/let [~@bs] (expr ~e))])))
                 (c/into (c/array-map)))]
    `(. (Language/Let ~fauna-bindings)
        (in (c/let [~@let-bindings]
              (jepsen.faunadb.query/do ~@body))))))

(defn abort
  [reason]
  (Language/Abort reason))

(defn if
  "If expression; takes a condition, true branch, else branch"
  ([c t]
   (jepsen.faunadb.query/if c t null))
  ([c t e]
   (Language/If (expr c) (expr t) (expr e))))

(defn when
  "Single-tailed if + do"
  ([c & ts]
   (jepsen.faunadb.query/if c (do* ts))))

(defn cond
  "Multi-tailed if. Like clojure cond, but also supports an odd arity, in which
  case the final clause is the default."
  [& clauses]
  (c/condp c/= (c/count clauses)
    0 null
    1 (expr (c/first clauses))
    (jepsen.faunadb.query/if (expr (c/first clauses))
      (expr (c/second clauses))
      (c/apply cond (c/next (c/next clauses))))))

; Functions

(defn at
  "Perform the given query at the given timestamp."
  [ts, e]
  (Language/At (expr ts) (expr e)))

(defn time
  "Construct a timestamp"
  [e]
  (Language/Time (expr e)))

(defn create-class
  "Creates a class, given a configuration object."
  [p]
  (Language/CreateClass (expr p)))

(defn create-index
  "Creates an index, given a configuration object."
  [p]
  (Language/CreateIndex (expr p)))

(defn create
  "Creates an instance, given a Ref and instance params."
  [ref params]
  (Language/Create ref (expr params)))

(defn paginate
  "Paginates an expression. Options are:

      :before - Fetch the page before this one
      :after  - Fetch the page after this one
      :ts     - Fetch at this timestamp
      :size   - Return this many elements per page"
  ([e] (paginate e {}))
  ([e opts]
   (c/assert (c/map? opts))
   (c/let [p (Language/Paginate (expr e))
           p (c/if-let [s (:size opts)]   (.size p    (c/int s))  p)
           p (c/if-let [a (:after opts)]  (.after p   (expr a))   p)
           p (c/if-let [b (:before opts)] (.before p  (expr b))   p)
           p (c/if-let [t (:ts opts)]     (.ts p      (expr t))   p)]
     p)))

(defn match
  "Matches everything in an index, or everything matching the given term"
  ([index]
   (Language/Match (expr index)))
  ([index term]
   (Language/Match (expr index) (expr term))))

(defn for-each
  [c l]
  (Language/Foreach (expr c) (expr l)))

(defn map
  [c l]
  (Language/Map (expr c) (expr l)))

(defn foreach
  "For each element in c, call (l c) for side effects"
  [c l]
  (Language/Foreach (expr c) (expr l)))

(defn exists?
  [r]
  (Language/Exists (expr r)))

(defn non-empty?
  [coll]
  (Language/IsNonEmpty (expr coll)))

(defn delete
  [r]
  (Language/Delete (expr r)))

(defn get
  [r]
  (Language/Get (expr r)))

(defn update
  [r data]
  (Language/Update (expr r) (expr data)))

(defn select
  ([path e]
   (Language/Select (expr path) (expr e)))
  ([path e default]
   (Language/Select (expr path) (expr e) (expr default))))

(defn union*
  "Set union"
  [sets]
  (Language/Union (c/mapv expr sets)))

(defn union
  [& sets]
  (union* sets))

(defn intersection*
  "Set intersection"
  [sets]
  (Language/Intersection (c/mapv expr sets)))

(defn intersection
  [& sets]
  (intersection* sets))

(defn -
  [& exprs]
  (Language/Subtract (c/mapv expr exprs)))

(defn +
  [& exprs]
  (Language/Add (c/mapv expr exprs)))

(defn not
  [e]
  (Language/Not (expr e)))

(defn or
  [& exprs]
  (Language/Or (c/mapv expr exprs)))

(defn and
  [& exprs]
  (Language/And (c/mapv expr exprs)))

(defn <
  [& exprs]
  (Language/LT (c/mapv expr exprs)))

(defn =
  [& exprs]
  (Language/Equals (c/mapv expr exprs)))

; Histories

(defn events
  "Returns a history of an instance's data, given a refset (is a ref a refset?)"
  [refset]
  (Language/Events (expr refset)))

(defn singleton
  [e]
  (Language/Singleton (expr e)))
