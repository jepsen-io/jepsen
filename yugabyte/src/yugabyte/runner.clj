(ns yugabyte.runner
  "Runs YugaByteDB tests."
  (:gen-class)
  (:require
            [clojure.tools.logging :refer :all]
            [jepsen.cli :as cli]
            [yugabyte.core]
            [yugabyte.single-row-inserts]
  )
)

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn yugabyte.single-row-inserts/test})
                   (cli/serve-cmd))
            args))
