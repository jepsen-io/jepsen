(ns jepsen.common-test
  "Support functions for writing tests."
  (:require [clojure.tools.logging :refer :all]
            [jepsen.store :as store]
            [unilog.config :as unilog]))

(defn quiet-logging
  "Quiets down logging"
  [f]
  (unilog/start-logging!
    {:level     "info"
     :console   false
     :appenders [store/console-appender]
     :overrides (merge store/default-logging-overrides
                       {"clj-ssh.ssh"         :error
                        "jepsen.db"           :error
                        "jepsen.core"         :error
                        "jepsen.control.util" :error
                        "jepsen.independent"  :error
                        "jepsen.generator"    :error
                        "jepsen.lazyfs"       :error
                        "jepsen.os.debian"    :error
                        "jepsen.store"        :error
                        "jepsen.util"         :error
                        "net.schmizz.sshj.transport" :error
                        })})
  (f)
  (store/stop-logging!))
