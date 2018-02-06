(ns jepsen.dgraph.support
  (:require [jepsen [db :as db]
                    [control :as c]]
            [jepsen.control.util :as cu]))

(def dir "/opt/dgraph")

(defn db
  "Sets up dgraph at the given version."
  []
  (reify db/DB
    (setup! [_ test node]
      (cu/install-archive!
        (str "https://github.com/dgraph-io/dgraph/releases/download/v"
             (:version test) "/dgraph-darwin-amd64.tar.gz")
        dir))

    (teardown! [_ test node]

      )

    db/LogFiles
    (log-files [_ test node]
      [])))
