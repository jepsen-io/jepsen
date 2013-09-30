(ns jepsen.radix
  "Constructs radix trees.")

(defn radix-tree
  []
  {})

(defn assoc-tree
  "Associates a sequential collection of elements into the tree."
  [t path]
  (if (empty? path)
    ; We're at the end of the collection; return the tree node.
    t

    ; Merge the first collection element into this level of the tree and
    ; recur.
    (let [[node & more] path]
      (assoc t node (assoc-tree (get t node) more)))))
