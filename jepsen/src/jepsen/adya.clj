(ns jepsen.adya
  "Generators and checkers for tests of Adya's proscribed behaviors for
  weakly-consistent systems. See http://pmg.csail.mit.edu/papers/adya-phd.pdf"
  (:require [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [knossos.model :as model]
            [knossos.op :as op]))

(defn g2-gen
  "With concurrent, unique keys, emits pairs of :insert ops of the form [key
  [a-id b-id]], where one txn has a-id and the other has b-id. a-id and b-id
  are globally unique. Only two insert ops are generated for any given key.
  Keys and ids are positive integers.

  G2 clients use two tables:

      create table a (
        id    int primary key,
        key   int,
        value int
      );
      create table b (
        id    int primary key,
        key   int,
        value int
      );

  G2 clients take operations like {:f :insert :value [key [a-id nil]]}, and in
  a single transaction, perform a read of tables a and b like so:

      select * from a where key = ? and value % 3 = 0

  and fail if either query returns more than zero rows. If both tables are
  empty, the client should insert a row like

      {:key key :id a-id :value 30}

  into table a, if a-id is present. If b-id is present, insert into table b
  instead. Iff the insert succeeds, return :type :ok with the operation value
  unchanged."
  []
  (let [ids (atom 0)]
    (independent/concurrent-generator
      2
      (range)
      (fn [k]
        (gen/seq
          [(fn [_  _]
             {:type :invoke :f :insert :value [nil (swap! ids inc)]})
           (fn [_  _]
             {:type :invoke :f :insert :value [(swap! ids inc) nil]})])))))

(defn g2-checker
  "Verifies that at most one :insert completes successfully for any given key."
  []
  (reify checker/Checker
    (check [this test model history opts]
      ; There should be at most one successful insert for any given key
      (let [keys (reduce (fn [m op]
                           (if (= :insert (:f op))
                             (let [k (key (:value op))]
                               (if (= :ok (:type op))
                                 (update m k (fnil inc 0))
                                 (update m k #(or % 0))))
                             m))
                         {}
                         history)
            insert-count (->> keys
                              (filter (fn [[k cnt]] (pos? cnt)))
                              count)
            illegal (->> keys
                         (keep (fn [[k cnt :as pair]]
                                 (when (< 1 cnt) pair)))
                         (into (sorted-map)))]
        {:valid?        (empty? illegal)
         :key-count     (count keys)
         :legal-count   (- insert-count (count illegal))
         :illegal-count (count illegal)
         :illegal       illegal}))))
