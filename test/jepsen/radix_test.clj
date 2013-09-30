(ns jepsen.radix-test
  (:use clojure.test
        jepsen.radix))

(deftest assoc-tree-test
  (are [t coll res] (= (assoc-tree t coll) res)
       nil nil
       nil

       nil [1]
       {1 nil}

       nil [1 2]
       {1 {2 nil}}

       nil [1 2 3 4]
       {1 {2 {3 {4 nil}}}}

       {1 {2 nil}} [1 2]
       {1 {2 nil}}

       {1 {2 nil}} [1 2 3]
       {1 {2 {3 nil}}}

       {1 {2 nil}} [1 4]
       {1 {2 nil
           4 nil}}

       {1 {2 nil}} [3 1 2]
       {1 {2 nil}
        3 {1 {2 nil}}}
  ))
