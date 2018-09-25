(ns jepsen.faunadb.query
  "A nice clojure interface for FaunaDB-JVM"
  (:import com.faunadb.client.query.Language)
  (:import com.faunadb.client.query.Expr))

(def Null
  (Language/Null))

; TODO: Exclude clojure, standardize cases and names, qualify references

(defn v
  [value]
  (Language/Value value))

(defn Ref
  [c i]
  (Language/Ref c (v i)))

(defn ClassRef
  [c]
  (Language/Class c))

(defn IndexRef
  [c]
  (Language/Index c))

(defn CreateClass
  [p]
  (Language/CreateClass p))

(defn CreateIndex
  [p]
  (Language/CreateIndex p))

(defn Create
  [r p]
  (Language/Create r p))

(defn Lambda
  [a e]
  (Language/Lambda a e))

(defn Paginate
  ([e] (Language/Paginate e))
  ([e a] (if (= Null a)
           (Paginate e)
           (. (Paginate e) (after a)))))

(defn Match
  [e]
  (Language/Match e))

(defn Do
  [e & es]
  (Language/Do (flatten (cons e es))))

(defn Foreach
  [c l]
  (Language/Foreach c l))

(defn Map
  [c l]
  (Language/Map c l))

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
  [& vs]
  (Language/Obj (into {} (map vec (partition 2 vs)))))

(defn Arr
  [v & vs]
  (Language/Arr (flatten (cons v vs))))

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
  [bindings expr]
  (. (Language/Let bindings) (in expr)))

(defn Subtract
  [& exprs]
  (Language/Subtract exprs))

(defn Add
  [& exprs]
  (Language/Add exprs))

(defn Or
  [& exprs]
  (Language/Or exprs))

(defn LessThan
  [& exprs]
  (Language/LT exprs))

(defn Var
  [n]
  (Language/Var n))

(defn Abort
  [reason]
  (Language/Abort reason))

(defn Equals
  [& exprs]
  (Language/Equals exprs))
