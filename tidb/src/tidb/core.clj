(ns tidb.core
  "Runs TiDB tests. Provides exit status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen.cli :as jc]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.core :as jepsen]
            [tidb.nemesis :as nemesis]
            [tidb.bank :as bank]
            [tidb.sequential :as sequential]
            [tidb.sets :as sets]
            [tidb.register :as register]))

(def tests
  "A map of test names to test constructors."
  {"bank"            bank/test
   "bank-multitable" bank/multitable-test
   "sets"            sets/test
   "sequential"      sequential/test
   "register"        register/test})

(def oses
  "Supported operating systems"
  {"debian" debian/os
   "none"   os/noop})

(def nemeses
  "Supported nemeses"
  {"none"                       `(nemesis/none)
   "parts"                      `(nemesis/parts)
   "majority-ring"              `(nemesis/majring)
   "start-stop-2"               `(nemesis/startstop 2)
   "start-kill-2"               `(nemesis/startkill 2)})

(def opt-spec
  "Command line options for tools.cli"
  [(jc/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                    [`(nemesis/none)]
                    nemeses)

   (jc/repeated-opt nil "--nemesis2 NAME" "An additional nemesis to mix in"
                    [nil]
                    nemeses)

   ["-o" "--os NAME" "debian, or none"
    :default debian/os
    :parse-fn oses
    :validate [identity (jc/one-of oses)]]

   (jc/repeated-opt "-t" "--test NAME" "Test(s) to run" [] tests)

   [nil "--recovery-time SECONDS"
    "How long to wait for cluster recovery before final ops."
    :default 60
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   (jc/tarball-opt "http://download.pingcap.org/tidb-latest-linux-amd64.tar.gz")])

(defn log-test
  [t]
  (info "Testing\n" (with-out-str (pprint t)))
  t)

(defn nemesis-product
  "The cartesian product of collections of symbols to nemesis functions c1 and
  c2, restricted to remove:

    - Duplicate orders (a,b) (b,a)
    - Pairs of clock-skew nemeses
    - Identical nemeses"
  [c1 c2]
  (->> (for [n1 c1, n2 c2] [n1 n2])
       (reduce (fn [[pairs seen] [n1 n2 :as pair]]
                 (if (or (= n1 n2)
                         (and (:clocks (eval n1)) (:clocks (eval n2)))
                         (seen #{n1 n2}))
                   [pairs seen]
                   [(conj pairs pair) (conj seen #{n1 n2})]))
               [[] #{}])
       first))

(defn test-cmd
  []
  {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
           :opt-fn (fn [parsed]
                     (-> parsed
                         jc/test-opt-fn
                         jc/validate-tarball
                         (jc/rename-options {:nemesis   :nemeses
                                             :nemesis2  :nemeses2
                                             :test      :test-fns})))
           :usage (jc/test-usage)
           :run (fn [{:keys [options]}]
                  (pprint options)
                  (doseq [i        (range (:test-count options))
                          test-fn  (:test-fns options)
                          [n1 n2]  (nemesis-product (:nemeses options)
                                                    (:nemeses2 options))]
                    (let [nemesis  (nemesis/compose [(eval n1) (eval n2)])
                          test (-> options
                                   (dissoc :test-fns :nemeses :nemeses2)
                                   (assoc :nemesis nemesis)
                                   test-fn
                                   log-test
                                   jepsen/run!)]
                      (when-not (:valid? (:results test))
                        (System/exit 1)))))}})

(defn -main
  [& args]
  (jc/run! (merge (jc/serve-cmd)
                  (test-cmd))
           args))
