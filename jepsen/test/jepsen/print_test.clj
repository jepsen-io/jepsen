(ns jepsen.print-test
  (:require [clojure [pprint]
                     [test :refer :all]]
            [jepsen [history :as h]
                    [print :as p]]))

; We don't want to print out jepsen.history.op everywhere; it's much harder to
; read
(deftest ^:focus op-pprint-test
  (let [op (h/op {:index 0
                  :time 1
                  :process 2
                  :type :ok
                  :f :read
                  :value "hi"})]
    (testing "Clojure pprint"
      (is (= "{:process 2, :type :ok, :f :read, :value \"hi\", :index 0, :time 1}\n"
             (with-out-str (clojure.pprint/pprint op)))))

    (testing "Jepsen Fipp pprint"
      (is (= "{:index 0, :time 1, :type :ok, :process 2, :f :read, :value \"hi\"}\n"
             (with-out-str (p/pprint op)))))))
