(ns jepsen.crate.runner
  "Runs Crate tests. Provides exit status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen.cli :as jc]
            [jepsen.core :as jepsen]
            [jepsen.crate.version-divergence :as version-divergence]
            [jepsen.crate.lost-updates :as lost-updates]
            [jepsen.crate.dirty-read :as dirty-read]
  )
)

(def tests
  "A map of test names to test constructors."
  {"version-divergence" version-divergence/test
   "lost-updates" lost-updates/test
   "dirty-read" dirty-read/test
  }
)

(def es-ops
  "A map of either none or a mix of operations to be executed by the es client"
  {"none" #{}
   "rws"  #{:read :write :strong-read}
   "rw"   #{:read :write}
   "rs"   #{:read :strong-read}
   "ws"   #{:write :strong-read}
   "r"    #{:read}
   "w"    #{:write}
   "s"    #{:strong-read}
  }
)

(def opt-spec
  "Command line options for tools.cli"
  [
   (jc/repeated-opt "-t" "--test NAME" "Test(s) to run" [] tests)

   [nil "--es-ops ES_OPS"
    "Operations to run against the ES client, e.g. none or mix"
    :default #{}
    :parse-fn es-ops
    :validate [identity (jc/one-of es-ops)]
    ]

   (jc/tarball-opt "https://cdn.crate.io/downloads/releases/crate-2.2.0.tar.gz")
  ]
)

(defn log-test
  [t]
  (info "Testing\n" (with-out-str (pprint t)))
  t)

(defn test-cmd
  []
  {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
           :opt-fn (fn [parsed]
                     (-> parsed
                         jc/test-opt-fn
                         jc/validate-tarball
                         (jc/rename-options {:test   :test-fns})
                         ))
           :usage (jc/test-usage)
           :run (fn [{:keys [options]}]
                  (info "Test options:\n"
                             (with-out-str (pprint options)))
                  (doseq [i        (range (:test-count options))
                          test-fn  (:test-fns options)]
                    (let [test (-> options
                                   (dissoc :test-fns)
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
