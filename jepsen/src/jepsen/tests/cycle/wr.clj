(ns jepsen.tests.cycle.wr
  "A test which looks for cycles in write/read transactions. Writes are assumed
  to be unique, but this is the only constraint. See elle.rw-register for docs."
  (:refer-clojure :exclude [test])
  (:require [elle [rw-register :as r]]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [store :as store]]))

(defn gen
  "Wrapper around elle.rw-register/gen."
  [opts]
  (r/gen opts))

(defn checker
  "Full checker for write-read registers. See elle.rw-register for options."
  ([]
   (checker {}))
  ([opts]
   (reify checker/Checker
     (check [this test history checker-opts]
       (r/check (assoc opts :directory
                       (.getCanonicalPath
                         (store/path! test (:subdirectory checker-opts) "elle")))
                history)))))

(defn test
  "A partial test, including a generator and a checker. You'll need to provide a client which can understand operations of the form:

    {:type :invoke, :f :txn, :value [[:r 3 nil] [:w 3 6]}

  and return completions like:

    {:type :ok, :f :txn, :value [[:r 3 1] [:w 3 6]]}

  Where the key 3 identifies some register whose value is initially 1, and
  which this transaction sets to 6.

  Options are passed directly to elle.rw-register/check and
  elle.rw-register/gen; see their docs for full options."
  [opts]
  {:generator (gen opts)
   :checker   (checker opts)})
