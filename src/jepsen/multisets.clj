(ns jepsen.multisets)

(deftype MultiSet [elements]
  clojure.lang.Seqable
  (seq [_]
    (->> elements
         (map (fn [[k count]] (repeat count k)))
         concat))
  
  clojure.lang.IPersistentCollection
  (count [_]      (reduce + (vals elements)))
  (cons  [_ k]    (MultiSet. (assoc elements k (inc (get elements k 0)))))
  (empty [_]      (MultiSet. (empty elements)))
  (equiv [this o] (and (instance? MultiSet o)
                       (= elements (.elements o))))

  clojure.lang.IPersistentSet
  (get [_ k]      (get elements k))
  (contains [_ k] (contains? elements k))
  (disjoin [_ k]  (MultiSet. (let [count (dec (get elements k 0))]
                               (if (pos? count)
                                 (assoc elements k count)
                                 (dissoc elements k))))))

(defn multiset [& elements]
  (into (MultiSet. (sorted-map)) elements))
