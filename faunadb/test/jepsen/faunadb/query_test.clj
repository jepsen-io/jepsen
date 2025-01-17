(ns jepsen.faunadb.query-test
  (:require [jepsen.faunadb.query :as q]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]])
  (:import (com.faunadb.client.query Language
                                     Expr)))

(deftest expr-test
  (testing "nil"
    (is (= (Language/Null) (q/expr nil))))
  (testing "numbers"
    (is (= (Language/Value 2) (q/expr 2))))
  (testing "strings"
    (is (= (Language/Value "foo") (q/expr "foo"))))
  (testing "booleans"
    (is (= (Language/Value false) (q/expr false)))
    (is (= (Language/Value true) (q/expr true))))

  ; lol these don't implement equality
  ; (testing "arrays"
  ;  (is (= (Language/Arr [])) (q/expr [])))

  ; Not comparable either
  ;(testing "maps"
    ;(is (= (Language/Obj {"name" (Language/Value "accounts")})
    ;       (q/expr {:name "accounts"}))))
  )
