(ns jepsen.control.sshj
  "An sshj-backed control Remote. Experimental; I'm considering replacing
  jepsen.control's use of clj-ssh with this instead."
  (:require [byte-streams :as bs]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.jcraft.jsch.agentproxy AgentProxy
                                       ConnectorFactory)
           (com.jcraft.jsch.agentproxy.sshj AuthAgent)
           (net.schmizz.sshj SSHClient)
           (net.schmizz.sshj.common IOUtils)
           (net.schmizz.sshj.connection.channel.direct Session)
           (net.schmizz.sshj.userauth UserAuthException)
           (net.schmizz.sshj.userauth.method AuthMethod)
           (net.schmizz.sshj.xfer FileSystemFile)
           (java.io IOException)
           (java.util.concurrent Semaphore
                                 TimeUnit)))

(defn auth-methods
  "Returns a list of AuthMethods we can use for logging in via an AgentProxy."
  [^AgentProxy agent]
  (map (fn [identity]
         (AuthAgent. agent identity))
    (.getIdentities agent)))

(defn ^AgentProxy agent-proxy
  []
  (-> (ConnectorFactory/getDefault)
      .createConnector
      AgentProxy.))

(defn auth!
  "Tries a bunch of ways to authenticate an SSHClient. We start with the given
  key file, if provided, then fall back to general public keys, then fall back
  to username/password."
  [^SSHClient c]
  (or ; Try given key
      (when-let [k c/*private-key-path*]
        (.authPublickey c c/*username* (into-array [k]))
        true)

      ; Try agent
      (try
        (let [agent-proxy (agent-proxy)
              methods (auth-methods agent-proxy)]
          (.auth c c/*username* methods)
          true)
        (catch UserAuthException e
          false))

      ; Fall back to standard id_rsa/id_dsa keys
      (try (.authPublickey c ^String c/*username*)
           true
           (catch UserAuthException e
             false))

      ; OK, standard keys didn't work, try username+password
      (.authPassword c c/*username* c/*password*)))

(defrecord SSHJRemote [concurrency-limit
                       ^SSHClient client
                       ^Semaphore semaphore]
  jepsen.control/Remote
  (connect [this host]
    (try+ (let [c (doto (SSHClient.)
                    (.loadKnownHosts)
                    (.connect host c/*port*)
                    auth!)]
            (assoc this
                   :client c
                   :semaphore (Semaphore. concurrency-limit true)))
          (catch Exception e
            (throw+ (assoc (c/debug-data)
                           :type    :jepsen.control/session-error
                           :message "Error opening SSH session. Verify username, password, and node hostnames are correct."
                           :host    host)))))

  (disconnect! [this]
    (when-let [c client]
      (.close c)))

  (execute! [this action]
  ;  (info :permits (.availablePermits semaphore))
    (.acquire semaphore)
    (try
      (with-open [session (.startSession client)]
        (let [cmd (.exec session (:cmd action))]
          ; Feed it input
          (when-let [input (:in action)]
            (let [stream (.getOutputStream cmd)]
              (bs/transfer input stream)))
          ; Wait on command
          (.join cmd)
          ; Return completion
          (assoc action
                 :out   (.toString (IOUtils/readFully (.getInputStream cmd)))
                 :err   (.toString (IOUtils/readFully (.getErrorStream cmd)))
                 ; There's also a .getExitErrorMessage that might be interesting
                 ; here?
                 :exit  (.getExitStatus cmd))))
      (finally
        (.release semaphore))))

  (upload! [this local-paths remote-path more]
    (with-open [sftp (.newSFTPClient client)]
      (.put sftp (FileSystemFile. local-paths) remote-path)))

  (download! [this remote-paths local-path more]
    (with-open [sftp (.newSFTPClient client)]
      (.get sftp remote-paths (FileSystemFile. local-path)))))

(def concurrency-limit
  "OpenSSH has a standard limit of 10 concurrent channels per connection.
  However, commands run in quick succession with 10 concurrent *also* seem to
  blow out the channel limit--perhaps there's an asynchronous channel teardown
  process. We set the limit a bit lower here. This is experimentally determined
  by running jepsen.control-test's integration test... <sigh>"
  6)

(defn remote
  "Constructs an SSHJ remote."
  []
  (SSHJRemote. concurrency-limit nil nil))
