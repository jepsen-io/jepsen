(ns jepsen.control.util-grepkill-test
  "grepkill! tests which don't need a DB node: we stub out exec and check the
  command it would have run."
  (:require [clojure.test :refer :all]
            [jepsen.control :as c]
            [jepsen.control.util :as util]))

(defn grepkill-cmds
  "Calls grepkill! with the given args, recording every command it passes to
  exec. Returns a vector of those commands' argument vectors."
  [& args]
  (let [calls (atom [])]
    (with-redefs [c/exec (fn [& cmd] (swap! calls conj (vec cmd)) "")]
      (apply util/grepkill! args))
    @calls))

(deftest grepkill-test
  (testing "with a signal"
    (is (= [[:pgrep :-f :--ignore-ancestors "foo"
             c/| :xargs :--no-run-if-empty :kill "-stop"]]
           (grepkill-cmds :stop "foo"))))

  (testing "defaults to SIGKILL"
    (is (= [[:pgrep :-f :--ignore-ancestors "foo"
             c/| :xargs :--no-run-if-empty :kill "-9"]]
           (grepkill-cmds "foo")))))
