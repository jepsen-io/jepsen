Let's start with the set of operations, represented as [transaction type variable] tuples.

```clj
user=> (def ops (for [t [1 2], op ['r 'w], var ['x 'y]] [t op var]))
([1 r x] [1 r y] [1 w x] [1 w y] [2 r x] [2 r y] [2 w x] [2 w y])
```

And expand that set to all possible orders:

```clj
user=> (require '[clojure.math.combinatorics :as c])
user=> (count (c/permutations ops))
40320
```

Too many for a direct case analysis, but because T<sub>1</sub> and T<sub>2</sub> are equivalent, we can eliminate half of the histories by symmetry.

```clj
user=> (def symmetry-reduction (partial filter #(= 1 (ffirst %))))
user=> (->> ops c/permutations symmetry-reduction count)
20160
```

Each transaction's operations must occur in order:

```clj
(defn in-order? [txn-history] (= (map rest txn-history) [ ['r 'x] ['r 'y] ['w 'x] ['w 'y]]))
(def in-txn-order (partial filter #(->> % (group-by first) vals (every? in-order?))))
user=> (->> ops c/permutations symmetry-reduction in-txn-order count)
35
```

Because reads commute, we can reduce the space further by collapsing all contiguous ranges of reads.

```clj
user=> (defn ordered? [xs] (= (sort xs) xs))

user=> (defn read? [ [t op var]] (= op 'r))

user=> (defn runs [pred coll]
         (let [ [runs run] (reduce (fn [ [runs run] x]
                                    (if (pred x)
                                      (if run
                                        [runs (conj run x)]
                                        [runs [x]])
                                      (if run
                                        [(conj runs run) nil]
                                        [runs nil])))
                                  [ [] nil]
                                  coll)]
           (if run (conj runs run) runs)))

user=> (def read-degeneracy (partial filter #(every? ordered? (runs read? %))))

user=> (->> ops c/permutations symmetry-reduction in-txn-order read-degeneracy count)
19
```
