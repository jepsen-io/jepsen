(ns yugabyte.runner
  "Runs YugaByteDB tests."
  (:gen-class)
  (:require
            [clojure.tools.logging :refer :all]
            [jepsen.cli :as cli]
            [yugabyte.core]
  )
)

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn yugabyte.core/yugabyte-test})
                   (cli/serve-cmd))
            args))
