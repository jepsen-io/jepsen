(ns jepsen.common-test
  "Support functions for writing tests."
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [os :as os]
                    [store :as store]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [unilog.config :as unilog]))

(defn quiet-logging
  "A fixture to quiet down logging, call f, then restore it."
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
                        "jepsen.tests.kafka"  :error
                        "jepsen.util"         :error
                        "net.schmizz.sshj"    :error
                        })})
  (f)
  (store/stop-logging!))

(defonce setup-os-run?
  (atom false))

(defn setup-os!
  "Fixture for tests. Sets up the OS on each remote node. Runs only once, to
  save time."
  [f]
  (when-not @setup-os-run?
    (let [test (assoc tests/noop-test :os debian/os)]
      (jepsen/with-sessions [test test]
        (c/with-all-nodes test
          (os/setup! (:os test) test c/*host*))))
    (reset! setup-os-run? true))
  (f))
