(ns jepsen.control.clj-ssh
  "A CLJ-SSH powered implementation of the Remote protocol."
  (:require [clojure.tools.logging :refer [info warn]]
            [clj-ssh.ssh :as ssh]
            [jepsen.control [core :as core]
                            [retry :as retry]
                            [scp :as scp]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent Semaphore)))

(def clj-ssh-agent
  "Acquiring an SSH agent is expensive and involves a global lock; we save the
  agent and re-use it to speed things up."
  (delay (ssh/ssh-agent {})))

(defn clj-ssh-session
  "Opens a raw session to the given connection spec"
  [conn-spec]
  (let [agent @clj-ssh-agent
        _     (when-let [key-path (:private-key-path conn-spec)]
                (ssh/add-identity agent {:private-key-path key-path}))]
    (doto (ssh/session agent
                       (:host conn-spec)
                       (select-keys conn-spec
                                    [:username
                                     :password
                                     :port
                                     :strict-host-key-checking]))
      (ssh/connect))))

(defmacro with-errors
  "Takes a conn spec, a context map, and a body. Evals body, remapping clj-ssh
  exceptions to :type :jepsen.control/ssh-failed."
  [conn context & body]
  `(try
     ~@body
     (catch com.jcraft.jsch.JSchException e#
       (if (or (= "session is down" (.getMessage e#))
               (= "Packet corrupt" (.getMessage e#)))
         (throw+ (merge ~conn ~context {:type :jepsen.control/ssh-failed}))
         (throw e#)))))

; TODO: pull out dummy logic into its own remote
(defrecord Remote [concurrency-limit
                   conn-spec
                   session
                   ^Semaphore semaphore]
  core/Remote
  (connect [this conn-spec]
    (assert (map? conn-spec)
            (str "Expected a map for conn-spec, not a hostname as a string. Received: "
                 (pr-str conn-spec)))
    (assoc this
           :conn-spec conn-spec
           :session (if (:dummy conn-spec)
                      {:dummy true}
                      (try+
                        (clj-ssh-session conn-spec)
                        (catch com.jcraft.jsch.JSchException _
                          (throw+ (merge conn-spec
                                         {:type :jepsen.control/session-error
                                          :message "Error opening SSH session. Verify username, password, and node hostnames are correct."})))))
           :semaphore (Semaphore. concurrency-limit true)))

  (disconnect! [_]
    (when-not (:dummy session) (ssh/disconnect session)))

  (execute! [_ ctx action]
    (with-errors conn-spec ctx
      (when-not (:dummy session)
        (.acquire semaphore)
        (try
          (ssh/ssh session action)
          (finally
            (.release semaphore))))))

  (upload! [_ ctx local-paths remote-path _opts]
    (with-errors conn-spec ctx
      (when-not (:dummy session)
        (apply ssh/scp-to session local-paths remote-path rest))))

  (download! [_ ctx remote-paths local-path _opts]
    (with-errors conn-spec ctx
      (when-not (:dummy session)
        (apply ssh/scp-from session remote-paths local-path rest)))))

(def concurrency-limit
  "OpenSSH has a standard limit of 10 concurrent channels per connection.
  However, commands run in quick succession with 10 concurrent *also* seem to
  blow out the channel limit--perhaps there's an asynchronous channel teardown
  process. We set the limit a bit lower here. This is experimentally determined
  for clj-ssh by running jepsen.control-test's integration test... <sigh>"
  8)

(defn remote
  "A remote that does things via clj-ssh."
  []
  (-> (Remote. concurrency-limit nil nil nil)
      ; We *can* use our own SCP, but shelling out is faster.
      scp/remote
      retry/remote))
