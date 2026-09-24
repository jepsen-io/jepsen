(ns jepsen.control.util-grepkill-test
  "grepkill! tests which don't need a DB node: we stub out exec and check the
  commands it would have run."
  (:require [clojure [string :as str]
                     [test :refer :all]]
            [jepsen.control :as c]
            [jepsen.control.util :as util]
            [clj-commons.slingshot :refer [try+ throw+]]))

(defn nonzero-exit
  "Throws the error exec throws when a command exits with the given status and
  stderr."
  [exit err]
  (throw+ {:type :jepsen.control/nonzero-exit, :exit exit, :err err}))

(defn grepkill-cmds
  "Calls grepkill! with the given args, stubbing exec. pgrep returns
  `pgrep-fn`'s result, and kill returns `kill-fn`'s; either may throw. Returns
  a vector of every command passed to exec, as argument vectors."
  [pgrep-fn kill-fn & args]
  (let [calls (atom [])]
    (with-redefs [c/exec (fn [& cmd]
                           (swap! calls conj (vec cmd))
                           (if (= :pgrep (first cmd))
                             (pgrep-fn)
                             (kill-fn)))]
      (apply util/grepkill! args))
    @calls))

(def pgrep
  "The pgrep command grepkill! runs for \"foo\"."
  [:pgrep :-f :--ignore-ancestors "foo"
   (c/lit "||") :test (c/lit "$?") :-eq 1])

(deftest grepkill-test
  (testing "kills every matching pid with the given signal"
    (is (= [pgrep ["/bin/kill" "-stop" "12" "34"]]
           (grepkill-cmds (constantly "12\n34") (constantly "") :stop "foo"))))

  (testing "defaults to SIGKILL"
    (is (= [pgrep ["/bin/kill" "-9" "12"]]
           (grepkill-cmds (constantly "12") (constantly "") "foo"))))

  (testing "no matches"
    ; The node turns pgrep's exit 1 into a clean exit with no output.
    (is (= [pgrep]
           (grepkill-cmds (constantly "") (constantly "") "foo"))))

  (testing "pgrep and sudo errors throw"
    (doseq [err ["pgrep: unrecognized option '--ignore-ancestors'"
                 "sudo: a password is required"]]
      (is (= err (try+ (grepkill-cmds #(nonzero-exit 1 err)
                                      (constantly "")
                                      "foo")
                       nil
                       (catch [:type :jepsen.control/nonzero-exit] e
                         (:err e)))))))

  (testing "kills many pids in batches"
    (let [pids (map str (range 2000))
          cmds (grepkill-cmds (constantly (str/join "\n" pids))
                              (constantly "")
                              "foo")]
      (is (= pgrep (first cmds)))
      (is (= [(into ["/bin/kill" "-9"] (take 1024 pids))
              (into ["/bin/kill" "-9"] (drop 1024 pids))]
             (rest cmds)))))

  (testing "processes which exit before we kill them are fine"
    (is (= [pgrep ["/bin/kill" "-9" "12" "34"]]
           (grepkill-cmds (constantly "12\n34")
                          #(nonzero-exit 1 "/bin/kill: (34): No such process\n")
                          "foo"))))

  (testing "other kill errors throw"
    (is (= 1 (try+ (grepkill-cmds
                     (constantly "12\n34")
                     #(nonzero-exit 1 (str "/bin/kill: (12): Operation not permitted\n"
                                           "/bin/kill: (34): No such process\n"))
                     "foo")
                   nil
                   (catch [:type :jepsen.control/nonzero-exit] e
                     (:exit e)))))))
