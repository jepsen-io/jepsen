(ns jepsen.cli-test
  (:require [clojure [test :refer :all]]
            [jepsen [cli :as cli]]))

(deftest without-default-for-test
  (is (= [["-w" "--workload NAME" "What workload should we run?"
           :parse-fn :foo]
          ["-a" "--another" "Some other option"
           :default false]
          [nil "--[no-]flag" "A boolean flag"]]
         (cli/without-defaults-for
           [:workload :flag]
           [["-w" "--workload NAME" "What workload should we run?"
             :default  :append
             :parse-fn :foo]
            ["-a" "--another" "Some other option"
             :default false]
            [nil "--[no-]flag" "A boolean flag"
             :default true]]))))


