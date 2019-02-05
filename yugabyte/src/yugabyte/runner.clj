(ns yugabyte.runner
  "Runs YugaByteDB tests."
  (:gen-class)
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer :all]
            [jepsen [core :as jepsen]
                    [cli :as cli]]
            [yugabyte [core]
                      [bank]
                      [counter]
                      [multi-key-acid]
                      [nemesis :as nemesis]
                      [set :as set]
                      [single-key-acid]
                      [single-row-inserts]]))

(def tests
  "A map of test names to test constructors."
  {"single-row-inserts" yugabyte.single-row-inserts/test
   "single-key-acid"    yugabyte.single-key-acid/test
   "multi-key-acid"     yugabyte.multi-key-acid/test
   "counter-inc"        yugabyte.counter/test-inc
   "counter-inc-dec"    yugabyte.counter/test-inc-dec
   "bank"               yugabyte.bank/test
   "bank-multitable"    yugabyte.bank/multitable-test
   "set"                yugabyte.set/test})

(def opt-spec
  "Additional command line options"
  [(cli/repeated-opt "-t" "--test NAME..." "Test(s) to run" [] tests)

   [nil "--nemesis NAME"
    (str "Nemesis to use, one of: "
         (clojure.string/join ", " (keys nemesis/nemeses)))
    :default "none"
    :validate [nemesis/nemeses (cli/one-of nemesis/nemeses)]]

   ["-o" "--os NAME" "Operating system: either centos or debian."
    :default  :centos
    :parse-fn keyword
    :validate [#{:centos :debian} "One of `centos` or `debian`"]]

   ["-d" "--db NAME" "Database variant: either community-edition (ce for short), or enterprise edition (ee for short)"
    :default :community-edition
    :parse-fn {"ce"                 :community-edition
               "community-edition"  :community-edition
               "ee"                 :enterprise-edition
               "enterprise-edition" :enterprise-edition}
    :validate [#{:community-edition :enterprise-edition}
               "Either community-edition or enterprise edition"]]

   ["-r" "--replication-factor INT" "Number of nodes in each Raft cluster."
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive integer"]]

   [nil "--yugabyte-ssh" "Override SSH options with hardcoded defaults for Yugabyte's internal testing environment"
    :default false]

   [nil "--version VERSION" "What version of Yugabyte to install"
    :default "1.1.10.0"]

   [nil "--url URL" "URL to Yugabyte tarball to install, has precedence over --version"
    :default nil]
   ])

(defn log-test
  [t attempt]
  (info "Testing" (:name t) "attempt #" attempt)
  t)

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing results."
  [& args]
  (cli/run!
   (merge
    (merge-with merge
                (cli/single-test-cmd
                 {:opt-spec opt-spec
                  ; :test-fn is required by single-test-cmd to construct :run, but :run will be overridden below in
                  ; order to support running multiple tests.
                  :test-fn  yugabyte.core/yugabyte-test})
                {"test" {:run (fn [{:keys [options]}]
                                (info "Options:\n" (with-out-str (pprint options)))
                                (let [invalid-results
                                      (->>
                                       (for [i       (range 1 (inc (:test-count options)))
                                             test-fn (:test options)]
                                         (let [_ (info :i i)
                                               test (-> options
                                                        (dissoc :test)
                                                        (assoc :nemesis-name (:nemesis options))
                                                        test-fn
                                                        (log-test i)
                                                        jepsen/run!)]
                                           [(:results test) i]))
                                       (filter #(->> % first :valid? true? not)))]
                                  (info :invalid-results invalid-results)
                                  (when-not (empty? invalid-results)
                                    ((info "Following tests have been failed:\n" (with-out-str (pprint invalid-results)))
                                      (System/exit 1)))
                                  ))}})
    (cli/serve-cmd))
   args))
