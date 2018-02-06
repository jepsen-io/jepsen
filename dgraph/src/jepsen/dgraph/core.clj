(ns jepsen.dgraph.core
  (:require [jepsen [cli :as cli]
                    [tests :as tests]]
            [jepsen.dgraph [support :as s]]))

(defn dgraph-test
  "Builds up a dgraph test map from CLI options."
  [opts]
  (merge tests/noop-test
         opts
         {:name (str "dgraph " (:version opts))
          :db   (s/db)}))

(def cli-opts
  "Additional command line options"
  [["-v" "--version VERSION" "What version number of dgraph should we test?"
    :default "1.0.2"]])

(defn -main
  "Handles command line arguments; running tests or the web server."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   dgraph-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
