(ns jepsen.tests.causal-reverse-test
  (:require [jepsen.tests.causal-reverse :refer :all]
            [clojure.test :refer :all]
            [jepsen [checker :as checker]
                    [history :as h]]))

(defn invoke [process f value] {:type :invoke, :process process, :f f, :value value})
(defn ok [process f value] {:type :ok, :process process, :f f, :value value})

(deftest casusal-reverse-test
  (testing "Can validate sequential histories"
    (let [c (checker)
          valid (h/history
                  [(invoke 0 :write 1)
                   (ok     0 :write 1)
                   (invoke 0 :write 2)
                   (ok     0 :write 2)
                   (invoke 0 :read nil)
                   (ok     0 :read [1 2])])
          one-without-two (h/history
                            [(invoke 0 :write 1)
                             (ok     0 :write 1)
                             (invoke 0 :write 2)
                             (ok     0 :write 2)
                             (invoke 0 :read nil)
                             (ok     0 :read [1])])
          two-without-one (h/history
                            [(invoke 0 :write 1)
                             (ok     0 :write 1)
                             (invoke 0 :write 2)
                             (ok     0 :write 2)
                             (invoke 0 :read nil)
                             (ok     0 :read [2])])
          bigger (h/history
                   [(invoke 0 :write 1)
                    (ok     0 :write 1)
                    (invoke 0 :write 2)
                    (ok     0 :write 2)
                    (invoke 0 :write 3)
                    (ok     0 :write 3)
                    (invoke 0 :write 4)
                    (ok     0 :write 4)
                    (invoke 0 :write 5)
                    (ok     0 :write 5)
                    (invoke 0 :read nil)
                    (ok     0 :read [1 2 3 4 5])])]
      (is (:valid?      (checker/check c nil valid nil)))
      (is (:valid?      (checker/check c nil one-without-two nil)))
      (is (not (:valid? (checker/check c nil two-without-one nil))))
      (is (:valid?      (checker/check c nil bigger nil)))))

  (testing "Can validate concurrent histories"
    (let [c (checker)
          concurrent1 (h/history
                        [(invoke 0 :write 2)
                         (invoke 0 :write 1)
                         (ok     0 :write 1)
                         (invoke 0 :read nil)
                         (ok     0 :write 2)
                         (ok     0 :read [1 2])])
          concurrent2  (h/history
                         [(invoke 0 :write 1)
                          (invoke 0 :write 2)
                          (ok     0 :write 1)
                          (invoke 0 :read nil)
                          (ok     0 :write 2)
                          (ok     0 :read [2 1])])]
      (is (:valid? (checker/check (checker) nil concurrent1 nil)))
      (is (:valid? (checker/check (checker) nil concurrent2 nil)))))

  ;; TODO Expand the checker to catch this sequential insert violation.
  #_(testing "Can detect reverse causal anomaly"
    (let [c (checker)
          reverse-causal-read (h/history [(invoke 0 :write 1)
                                          (ok     0 :write 1)
                                          (invoke 0 :write 2)
                                          (ok     0 :write 2)
                                          (invoke 0 :read nil)
                                          (ok     0 :read [2 1])])]
      (is (not (:valid? (checker/check c nil reverse-causal-read nil)))))))
