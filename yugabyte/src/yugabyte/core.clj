(ns yugabyte.core
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojurewerkz.cassaforte.client :as cassandra]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]
                    [util :as util :refer [meh timeout]]
            ]
            [jepsen.control.util :as cu]
            ))

(def master-log-dir  "/home/yugabyte/master/logs")
(def tserver-log-dir "/home/yugabyte/tserver/logs")

(def setup-lock (Object.))

(defn wait-for-recovery
  "Waits for the driver to report all nodes are up"
  [timeout-secs conn]
  (timeout (* 1000 timeout-secs)
           (throw (RuntimeException.
                   (str "Driver didn't report all nodes were up in "
                        timeout-secs "s - failing")))
           (while (->> (cassandra/get-hosts conn)
                       (map :is-up) and not)
             (Thread/sleep 500))))

(defn start!
  "Starts YugaByteDB."
  [node test]
  (info node "Starting YugaByteDB")
  (info (meh (c/exec (c/lit "if [[ -e /home/yugabyte/master/master.out ]]; then /home/yugabyte/bin/yb-server-ctl.sh master start; fi"))))
  (info (meh (c/exec (c/lit "/home/yugabyte/bin/yb-server-ctl.sh tserver start"))))
)

(defn stop!
  "Stops YugaByteDB."
  [node]
  (info node "Stopping YugaByteDB")
  (info (meh (c/exec (c/lit "/home/yugabyte/bin/yb-server-ctl.sh tserver stop; pkill -9 yb-tserver || true"))))
  (info (meh (c/exec (c/lit "if [[ -e /home/yugabyte/master/master.out ]]; then /home/yugabyte/bin/yb-server-ctl.sh master stop; pkill -9 yb-master || true; fi"))))
)

(defn wipe!
  "Shuts down YugaByteDB and wipes data."
  [node]
  (stop! node)
  (info node "Deleting data and log files")
  (meh (c/exec :rm :-r (c/lit (str "/mnt/d*/yb-data/master/*"))))
  (meh (c/exec :rm :-r (c/lit (str "/mnt/d*/yb-data/tserver/*"))))
  (meh (c/exec :mkdir "/mnt/d0/yb-data/master/logs"))
  (meh (c/exec :mkdir "/mnt/d0/yb-data/tserver/logs"))
)

(defn db
  "YugaByteDB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "Setup YugaByteDB " version)
      (start! node test)
; TODO - wait for all nodes up instead of sleep for each node.
      (Thread/sleep 5000)
    )

    (teardown! [_ test node]
      (info node "Tearing down YugaByteDB...")
      (wipe! node)
    )

    db/LogFiles
    (log-files [_ test node]
      (concat (cu/ls-full master-log-dir)
              (cu/ls-full tserver-log-dir)))
  )
)

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 1000000)})

(defn yugabyte-test
  [opts]
  (merge tests/noop-test
         opts
         {
          :ssh {
              :port 54422
              :private-key-path
                  (str (System/getenv "HOME") "/.yugabyte/yugabyte-dev-aws-keypair.pem")
              :strict-host-key-checking false
              :username "yugabyte"
          }
          :db      (db "x.y.z")
         }))
