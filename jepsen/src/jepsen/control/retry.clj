(ns jepsen.control.retry
  "SSH client libraries appear to be near universally-flaky. Maybe race
  conditions, maybe underlying network instability, maybe we're just doing it
  wrong. For whatever reason, they tend to throw errors constantly. The good
  news is we can almost always retry their commands safely! This namespace
  provides a Remote which wraps an underlying Remote in a jepsen.reconnect
  wrapper, catching certain exception classes and ensuring they're
  automatically retried."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt]
            [jepsen [reconnect :as rc]]
            [jepsen.control [core :as core]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def retries
  "How many times should we retry exceptions before giving up and throwing?"
  5)

(def backoff-time
  "Roughly how long should we back off when retrying, in ms?"
  100)

(defmacro with-retry
  "Takes a body. Evaluates body, retrying SSH exceptions."
  [& body]
  `(dt/with-retry [tries# retries]
     (try+
       ~@body
       (catch [:type :jepsen.control/ssh-failed] e#
         (if (pos? tries#)
           (do (Thread/sleep (+ (/ backoff-time 2) (rand-int backoff-time)))
               (~'retry (dec tries#)))
           (throw e#))))))

(defrecord Remote [remote conn]
  core/Remote
  (connect [this conn-spec]
    ; Construct a conn (a Reconnect wrapper) for the underlying remote, and
    ; open it.
    (let [conn (-> {:open (fn open []
                            (core/connect remote conn-spec))
                    :close core/disconnect!
                    :name  [:control (:host conn-spec)]
                    :log?  :minimal}
                   rc/wrapper
                   rc/open!)]
      (assoc this :conn conn)))

  (disconnect! [this]
    (rc/close! conn))

  (execute! [this context action]
    (with-retry
      (rc/with-conn [c conn]
        (core/execute! c context action))))

  (upload! [this context local-paths remote-path more]
    (with-retry
      (rc/with-conn [c conn]
        (core/upload! c context local-paths remote-path more))))

  (download! [this context remote-paths local-path more]
    (with-retry
      (rc/with-conn [c conn]
        (core/download! c context remote-paths local-path more)))))

(defn remote
  "Constructs a new Remote by wrapping another Remote in one which
  automatically catches and retries any exception of the form {:type
  :jepsen.control/ssh-failed}."
  [remote]
  (Remote. remote nil))
