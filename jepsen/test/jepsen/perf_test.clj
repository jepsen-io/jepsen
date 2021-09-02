(ns jepsen.perf-test
  (:refer-clojure :exclude [run!])
  (:use clojure.test)
  (:require [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.checker :as checker]
            [knossos.model :as model]))

(deftest perf-test
  (let [history [{:process 3, :type :invoke, :f :read}
                 {:process 4, :type :invoke, :f :cas, :value [3 3]}
                 {:process 1, :type :invoke, :f :read}
                 {:process 0, :type :invoke, :f :cas, :value [2 1]}
                 {:process 2, :type :invoke, :f :read}
                 {:value 0, :process 3, :type :ok, :f :read}
                 {:value 0, :process 1, :type :ok, :f :read}
                 {:process 3, :type :invoke, :f :read}
                 {:value 0, :process 2, :type :ok, :f :read}
                 {:process 2, :type :invoke, :f :read}
                 {:value 0, :process 3, :type :ok, :f :read}
                 {:value 0, :process 2, :type :ok, :f :read}
                 {:process 3, :type :invoke, :f :read}
                 {:value 0, :process 3, :type :ok, :f :read}
                 {:process 3, :type :invoke, :f :read}
                 {:value 0, :process 3, :type :ok, :f :read}
                 {:process 0, :type :fail, :f :cas, :value [2 1]}
                 {:process 4, :type :fail, :f :cas, :value [3 3]}
                 {:process 3, :type :invoke, :f :cas, :value [1 2]}
                 {:process 2, :type :invoke, :f :cas, :value [0 0]}
                 {:process 1, :type :invoke, :f :cas, :value [3 2]}
                 {:process 4, :type :invoke, :f :read}
                 {:process 0, :type :invoke, :f :cas, :value [1 0]}
                 {:process 3, :type :fail, :f :cas, :value [1 2]}
                 {:process 2, :type :ok, :f :cas, :value [0 0]}
                 {:value 0, :process 4, :type :ok, :f :read}
                 {:process 1, :type :fail, :f :cas, :value [3 2]}
                 {:process 1, :type :invoke, :f :read}
                 {:process 2, :type :invoke, :f :cas, :value [1 4]}
                 {:process 4, :type :invoke, :f :cas, :value [1 2]}
                 {:process 3, :type :invoke, :f :cas, :value [0 1]}
                 {:process 0, :type :fail, :f :cas, :value [1 0]}
                 {:value 0, :process 1, :type :ok, :f :read}
                 {:process 4, :type :fail, :f :cas, :value [1 2]}
                 {:process 2, :type :fail, :f :cas, :value [1 4]}
                 {:process 0, :type :invoke, :f :cas, :value [1 2]}
                 {:process 1, :type :invoke, :f :read}
                 {:process 4, :type :invoke, :f :read}
                 {:process 3, :type :ok, :f :cas, :value [0 1]}
                 {:process 0, :type :ok, :f :cas, :value [1 2]}
                 {:value 2, :process 1, :type :ok, :f :read}
                 {:value 2, :process 4, :type :ok, :f :read}
                 {:process 2, :type :invoke, :f :cas, :value [2 4]}
                 {:process 3, :type :invoke, :f :read}
                 {:process 0, :type :invoke, :f :read}
                 {:process 4, :type :invoke, :f :cas, :value [4 2]}
                 {:process 2, :type :ok, :f :cas, :value [2 4]}
                 {:value 2, :process 3, :type :ok, :f :read}
                 {:process 1, :type :invoke, :f :cas, :value [2 0]}
                 {:value 4, :process 0, :type :ok, :f :read}
                 {:process 4, :type :ok, :f :cas, :value [4 2]}
                 {:process 2, :type :invoke, :f :cas, :value [3 3]}
                 {:process 1, :type :ok, :f :cas, :value [2 0]}
                 {:process 3, :type :invoke, :f :cas, :value [4 4]}
                 {:process 0, :type :invoke, :f :cas, :value [2 1]}
                 {:process 1, :type :invoke, :f :read}
                 {:process 4, :type :invoke, :f :read}
                 {:process 3, :type :fail, :f :cas, :value [4 4]}
                 {:process 2, :type :fail, :f :cas, :value [3 3]}
                 {:value 0, :process 4, :type :ok, :f :read}
                 {:value 0, :process 1, :type :ok, :f :read}
                 {:process 3, :type :invoke, :f :cas, :value [2 4]}
                 {:process 2, :type :invoke, :f :read}
                 {:process 0, :type :fail, :f :cas, :value [2 1]}
                 {:process 4, :type :invoke, :f :read}
                 {:process 1, :type :invoke, :f :read}
                 {:process 3, :type :fail, :f :cas, :value [2 4]}
                 {:value 0, :process 2, :type :ok, :f :read}
                 {:process 0, :type :invoke, :f :cas, :value [4 1]}
                 {:value 0, :process 4, :type :ok, :f :read}
                 {:process 3, :type :invoke, :f :cas, :value [3 0]}
                 {:process 2, :type :invoke, :f :cas, :value [1 3]}
                 {:value 0, :process 1, :type :ok, :f :read}
                 {:process 4, :type :invoke, :f :read}
                 {:process 0, :type :fail, :f :cas, :value [4 1]}
                 {:process 2, :type :fail, :f :cas, :value [1 3]}
                 {:process 3, :type :fail, :f :cas, :value [3 0]}
                 {:process 0, :type :invoke, :f :cas, :value [0 1]}
                 {:process 1, :type :invoke, :f :read}
                 {:process 2, :type :invoke, :f :cas, :value [0 4]}
                 {:value 0, :process 4, :type :ok, :f :read}
                 {:value 1, :process 1, :type :ok, :f :read}
                 {:process 4, :type :invoke, :f :read}
                 {:process 3, :type :invoke, :f :cas, :value [2 1]}
                 {:process 2, :type :fail, :f :cas, :value [0 4]}
                 {:value 1, :process 4, :type :ok, :f :read}
                 {:process 0, :type :ok, :f :cas, :value [0 1]}
                 {:process 1, :type :invoke, :f :cas, :value [3 1]}
                 {:process 2, :type :invoke, :f :cas, :value [3 3]}
                 {:process 3, :type :fail, :f :cas, :value [2 1]}
                 {:process 4, :type :invoke, :f :read}
                 {:process 0, :type :invoke, :f :cas, :value [3 1]}
                 {:process 1, :type :fail, :f :cas, :value [3 1]}
                 {:process 2, :type :fail, :f :cas, :value [3 3]}
                 {:value 1, :process 4, :type :ok, :f :read}
                 {:process 0, :type :fail, :f :cas, :value [3 1]}
                 {:process 2, :type :invoke, :f :read}
                 {:process 3, :type :invoke, :f :cas, :value [4 4]}
                 {:process 4, :type :invoke, :f :cas, :value [3 1]}
                 {:process 1, :type :invoke, :f :read}
                 {:process 3, :type :fail, :f :cas, :value [4 4]}
                 {:process 0, :type :invoke, :f :read}
                 {:process 4, :type :fail, :f :cas, :value [3 1]}
                 {:value 1, :process 2, :type :ok, :f :read}
                 {:value 1, :process 1, :type :ok, :f :read}
                 {:process 3, :type :invoke, :f :read}
                 {:value 1, :process 0, :type :ok, :f :read}
                 {:process 4, :type :invoke, :f :read}
                 {:process 2, :type :invoke, :f :read}
                 {:value 1, :process 3, :type :ok, :f :read}
                 {:process 0, :type :invoke, :f :read}
                 {:process 3, :type :invoke, :f :read}
                 {:value 1, :process 2, :type :ok, :f :read}
                 {:value 1, :process 4, :type :ok, :f :read}
                 {:value 1, :process 0, :type :ok, :f :read}
                 {:value 1, :process 3, :type :ok, :f :read}
                 {:process 1, :type :invoke, :f :cas, :value [2 2]}
                 {:process 2, :type :invoke, :f :cas, :value [4 3]}
                 {:process 1, :type :fail, :f :cas, :value [2 2]}
                 {:process 2, :type :fail, :f :cas, :value [4 3]}]]
    (time
     (is (:valid? (checker/check (checker/linearizable {:model (model/->CASRegister 0)})
                                 {}
                                 history
                                 {}))))))
