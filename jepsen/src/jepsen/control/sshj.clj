(ns jepsen.control.sshj
  "An sshj-backed control Remote. Experimental; I'm considering replacing
  jepsen.control's use of clj-ssh with this instead."
  (:require [byte-streams :as bs]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :refer :all]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (net.schmizz.sshj SSHClient)
           (net.schmizz.sshj.common IOUtils)
           (net.schmizz.sshj.connection.channel.direct Session)
           (net.schmizz.sshj.userauth UserAuthException)
           (net.schmizz.sshj.xfer FileSystemFile)
           (java.io IOException)
           (java.util.concurrent TimeUnit)))

(defn auth!
  "Tries a bunch of ways to authenticate an SSHClient. We start with the given
  key file, if provided, then fall back to general public keys, then fall back
  to username/password."
  [^SSHClient c]
  ; Try given key
  (when-let [k *private-key-path*]
    (.authPublickey c *username* (into-array [k])))

  ; TODO: add SSH agent support using https://github.com/ymnk/jsch-agent-proxy/blob/master/examples/src/main/java/com/jcraft/jsch/agentproxy/examples/SshjWithAgentProxy.java?

  ; Fall back to standard id_rsa/id_dsa keys
  (try (.authPublickey c ^String *username*)
       (catch UserAuthException e
         ; OK, standard keys didn't work, try username+password
         (.authPassword c *username* *password*))))

(defrecord SSHJRemote [^SSHClient client]
  jepsen.control/Remote
  (connect [this host]
    (try+ (let [c (doto (SSHClient.)
                    (.loadKnownHosts)
                    (.connect host *port*)
                    auth!)]
            (assoc this :client c))
          (catch Exception e
            (throw+ (assoc (debug-data)
                           :type    :jepsen.control/session-error
                           :message "Error opening SSH session. Verify username, password, and node hostnames are correct."
                           :host    host)))))

  (disconnect! [this]
    (when-let [c client]
      (.close c)))

  (execute! [this action]
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
               :exit  (.getExitStatus cmd)))))

  (upload! [this local-paths remote-path more]
    (with-open [sftp (.newSFTPClient client)]
      (.put sftp (FileSystemFile. local-paths) remote-path)))

  (download! [this remote-paths local-path more]
    (with-open [sftp (.newSFTPClient client)]
      (.get sftp remote-paths (FileSystemFile. local-path)))))

(defn remote
  "Constructs an SSHJ remote."
  []
  (SSHJRemote. nil))
