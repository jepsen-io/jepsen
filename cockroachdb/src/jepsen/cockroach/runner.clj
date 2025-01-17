(ns jepsen.cockroach.runner
  "Runs CockroachDB tests. Provides exit status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen.cli :as jc]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.core :as jepsen]
            [jepsen.web :as web]
            [jepsen.cockroach :as cockroach]
            [jepsen.cockroach [adya :as adya]
                              [bank :as bank]
                              [comments :as comments]
                              [register :as register]
                              [monotonic :as monotonic]
                              [nemesis :as cln]
                              [sets :as sets]
                              [sequential :as sequential]]))

(def tests
  "A map of test names to test constructors."
  {"bank"                 bank/test
   "bank-multitable"      bank/multitable-test
   "comments"             comments/test
   "register"             register/test
   "monotonic"            monotonic/test
   "sets"                 sets/test
   "sequential"           sequential/test
   "g2"                   adya/g2-test})

(def oses
  "Supported operating systems"
  {"debian" debian/os
   "ubuntu" ubuntu/os
   "none"   os/noop})

(def nemeses
  "Supported nemeses"
  {"none"                       `(cln/none)
   "parts"                      `(cln/parts)
   "majority-ring"              `(cln/majring)
;   "small-skews"                `(cln/small-skews)
   "subcritical-skews"          `(cln/subcritical-skews)
   "critical-skews"             `(cln/critical-skews)
   "big-skews"                  `(cln/big-skews)
   "huge-skews"                 `(cln/huge-skews)
   "strobe-skews"               `(cln/strobe-skews)
   "split"                      `(cln/split)
;   "start-stop"                 `(cln/startstop 1)
   "start-stop-2"               `(cln/startstop 2)
;   "start-kill"                 `(cln/startkill 1)
   "start-kill-2"               `(cln/startkill 2)})

(def opt-spec
  "Command line options for tools.cli"
  [[nil "--force-download" "Always download HTTP/S tarballs"]

   ["-l" "--linearizable" "Whether to run cockroack in linearizable mode"
    :default false]

   (jc/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                    [`(cln/none)]
                    nemeses)

   (jc/repeated-opt nil "--nemesis2 NAME" "An additional nemesis to mix in"
                    [nil]
                    nemeses)

   ["-o" "--os NAME" "debian, ubuntu, or none"
    :default debian/os
    :parse-fn oses
    :validate [identity (jc/one-of oses)]]

   (jc/repeated-opt "-t" "--test NAME" "Test(s) to run" [] tests)

   [nil "--recovery-time SECONDS"
    "How long to wait for cluster recovery before final ops."
    :default 60
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   (jc/tarball-opt "https://binaries.cockroachdb.com/cockroach-beta-20170330.linux-amd64.tgz")])

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
                    ; Rehydrate test and run
                    (let [nemesis  (cln/compose [(eval n1) (eval n2)])
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
