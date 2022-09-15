(ns jepsen.tests.cycle.append
  "Detects cycles in histories where operations are transactions over named
  lists lists, and operations are either appends or reads. See elle.list-append
  for docs."
  (:refer-clojure :exclude [test])
  (:require [elle.list-append :as la]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [store :as store]]))

(defn checker
  "Full checker for append and read histories. See elle.list-append for
  options."
  ([]
   (checker {}))
  ([opts]
   (reify checker/Checker
     (check [this test history checker-opts]
       (la/check (assoc opts :directory
                        (.getCanonicalPath
                          (store/path! test (:subdirectory checker-opts) "elle")))
                 history)))))

(defn gen
  "Wrapper for elle.list-append/gen; as a Jepsen generator."
  [opts]
  (la/gen opts))

(defn test
  "A partial test, including a generator and checker. You'll need to provide a
  client which can understand operations of the form:

      {:type :invoke, :f :txn, :value [[:r 3 nil] [:append 3 2] [:r 3]]}

  and return completions like:

      {:type :ok, :f :txn, :value [[:r 3 [1]] [:append 3 2] [:r 3 [1 2]]]}

  where the key 3 identifies some list, whose value is initially [1], and
  becomes [1 2].

  Options are passed directly to elle.list-append/check and
  elle.list-append/gen; see their docs for full options."
  [opts]
  {:generator (gen opts)
   :checker   (checker opts)})
