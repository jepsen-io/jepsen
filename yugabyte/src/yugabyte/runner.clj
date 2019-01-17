(ns yugabyte.runner
  "Runs YugaByteDB tests."
  (:gen-class)
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer :all]
            [jepsen [core :as jepsen]
                    [cli :as cli]]
            [yugabyte [core]
                      [counter]
                      [multi-key-acid]
                      [nemesis :as nemesis]
                      [single-key-acid]
                      [single-row-inserts]
             ]
  )
)

(def tests
  "A map of test names to test constructors."
  {"single-row-inserts" yugabyte.single-row-inserts/test
   "single-key-acid" yugabyte.single-key-acid/test
   "multi-key-acid" yugabyte.multi-key-acid/test
   "counter-inc" yugabyte.counter/test-inc
   "counter-inc-dec" yugabyte.counter/test-inc-dec
   }
  )

(def opt-spec
  "Additional command line options"
  [
   (cli/repeated-opt "-t" "--test NAME..." "Test(s) to run" [] tests)

   [nil "--nemesis NAME"
    (str "Nemesis to use, one of: " (clojure.string/join ", " (keys nemesis/nemeses)))
    :default "none"
    :validate [identity (cli/one-of nemesis/nemeses)]]
  ]
)

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
                                         (let [test (-> options
                                                        (dissoc :test)
                                                        (assoc :nemesis-name (:nemesis options))
                                                        test-fn
                                                        (log-test i)
                                                        jepsen/run!)]
                                           [(:results test) i]))
                                       (filter #(->> % first :valid? true? not)))]
                                  (when-not (empty? invalid-results)
                                    ((info "Following tests have been failed:\n" (with-out-str (pprint invalid-results)))
                                      (System/exit 1)))
                                  ))}})
    (cli/serve-cmd))
   args))
