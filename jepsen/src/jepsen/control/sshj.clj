(ns jepsen.control.sshj
  "An sshj-backed control Remote. Experimental; I'm considering replacing
  jepsen.control's use of clj-ssh with this instead."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :refer :all]])
  (:import (net.schmizz.sshj SSHClient)
           (net.schmizz.sshj.common IOUtils)
           (net.schmizz.sshj.connection.channel.direct Session)
           (java.io IOException)
           (java.util.concurrent TimeUnit)))

(defrecord SSHJRemote [^SSHClient client, ^Session session]
  jepsen.control/Remote
  (connect [this host]
    (let [c (doto (SSHClient.)
                  (.loadKnownHosts)
                  (.connect host))
          s (.startSession c)]
      (assoc this :client c, :session s)))

  (disconnect! [this]
    (.close session)
    (.disconnect client))

  (execute! [this action]
    (info :execute action)
    nil)

  (upload! [this local-paths remote-path more])

  (download! [this remote-paths local-path more]))

(defn remote
  "Constructs an SSHJ remote."
  []
  (SSHJRemote. nil nil))
