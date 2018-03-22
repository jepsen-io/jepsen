(ns jepsen.faunadb.query
  "A nice clojure interface for FaunaDB-JVM"
  (:import com.faunadb.client.query.Language)
  (:import com.faunadb.client.query.Expr))

(defn v
  [value]
  (Language/Value value))

(defn Ref
  [c i]
  (Language/Ref c i))

(defn ClassRef
  [c]
  (Language/Class c))

(defn CreateClass
  [p]
  (Language/CreateClass p))

(defn Create
  [r p]
  (Language/Create r p))

(defn Do
  [& exprs]
  (Language/Do exprs))

(defn If
  [c t e]
  (Language/If c t e))

(defn Exists
  [r]
  (Language/Exists r))

(defn Delete
  [r]
  (Language/Delete r))

(defn Obj
  [k v]
  (Language/Obj k v))

(defn Arr
  [& vs]
  (Language/Arr (into-array Expr vs)))

(defn Get
  [r]
  (Language/Get r))

(defn Update
  [r data]
  (Language/Update r data))

(defn Select
  [path expr]
  (Language/Select path expr))

(defn Let
  [[bindings] expr]
  (Language/Let bindings expr))

(defn Subtract
  [& exprs]
  (Language/Subtract (into-array Expr exprs)))

(defn Add
  [& exprs]
  (Language/Add (into-array Expr exprs)))

(defn Or
  [& exprs]
  (Language/Or (into-array Expr exprs)))

(defn LessThan
  [& exprs]
  (Language/LT (into-array Expr exprs)))

(defn Var
  [n]
  (Language/Var n))

(defn Abort
  [reason]
  (Language/Abort reason))
