(ns jepsen.common-test
  "Support functions for writing tests."
  (:require [clojure.tools.logging :refer :all]
            [jepsen.store :as store]
            [unilog.config :as unilog]))

(defn quiet-logging
  "Quiets down logging"
  [f]
  (prn :start-logging)
  (unilog/start-logging!
    {:level     "info"
     :console   false
     :appenders [store/console-appender]
     :overrides (merge store/default-logging-overrides
                       {})})
  (f)
  (store/stop-logging!))
