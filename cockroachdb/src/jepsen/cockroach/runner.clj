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
            [jepsen.cockroach [bank :as bank]
                              [register :as register]
                              [monotonic :as monotonic]
                              [nemesis :as cln]
                              [sets :as sets]
                              [sequential :as sequential]]))

(def tests
  "A map of test names to test constructors."
  {"bank"                 bank/test
   "bank-multitable"      bank/multitable-test
   "register"             register/test
   "monotonic"            monotonic/test
   "monotonic-multitable" monotonic/multitable-test
   "sets"                 sets/test
   "sequential"           sequential/test})

(def oses
  "Supported operating systems"
  {"debian" debian/os
   "ubuntu" ubuntu/os
   "none"   os/noop})

(def nemeses
  "Supported nemeses"
  {"none"                       `cln/none
   "parts"                      `cln/parts
   "majority-ring"              `cln/majring
   "small-skews"                `cln/small-skews
   "subcritical-skews"          `cln/subcritical-skews
   "critical-skews"             `cln/critical-skews
   "big-skews"                  `cln/big-skews
   "huge-skews"                 `cln/huge-skews
   "strobe-skews"               `cln/strobe-skews
   "split"                      `cln/split
   "start-stop"                 `(cln/startstop 1)
   "start-stop-2"               `(cln/startstop 2)
   "start-kill"                 `(cln/startkill 1)
   "start-kill-2"               `(cln/startkill 2)
   "skews-start-kill-2"         `(cln/compose cln/subcritical-skews
                                              (cln/startkill 2))
   "majority-ring-start-kill-2" `(cln/compose cln/majring    (cln/startkill 2))
   "parts-skews"                `(cln/compose cln/parts      cln/small-skews)
   "parts-big-skews"            `(cln/compose cln/parts      cln/big-skews)
   "parts-strobe-skews"         `(cln/compose cln/parts      cln/strobe-skews)
   "parts-start-kill-2"         `(cln/compose cln/parts      (cln/startkill 2))
   "start-kill-2-strobe-skews" `(cln/compose (cln/startkill 2) cln/strobe-skews)
   "majority-ring-skews"        `(cln/compose cln/majring    cln/small-skews)
   "start-stop-skews"           `(cln/compose cln/startstop  cln/small-skews)})

(def opt-spec
  "Command line options for tools.cli"
  [[nil "--force-download" "Always download HTTP/S tarballs"]

   ["-l" "--linearizable" "Whether to run cockroack in linearizable mode"
    :default false]

   (jc/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                    [`cln/none]
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

   (jc/tarball-opt "https://binaries.cockroachdb.com/cockroach-beta-20160929.linux-amd64.tgz")])

(defn log-test
  [t]
  (info "Testing\n" (with-out-str (pprint t)))
  t)

(defn test-cmd
  []
  {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
           :opt-fn (fn [parsed]
                     (-> parsed
                         jc/validate-tarball
                         (jc/rename-options {:node    :nodes
                                             :nemesis :nemeses
                                             :test    :test-fns})
                         jc/read-nodes-file))
           :usage (jc/test-usage)
           :run (fn [{:keys [options]}]
                  (prn :running)
                  (pprint options)
                  (doseq [test-fn (:test-fns options)
                          nemesis (:nemeses options)
                          i       (range (:test-count options))]
                    ; Rehydrate test and run
                    (let [test (-> options
                                   (dissoc :test-fns)
                                   (assoc :nemesis (eval nemesis))
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
