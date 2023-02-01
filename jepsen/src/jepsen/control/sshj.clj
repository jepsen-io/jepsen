(ns jepsen.control.sshj
  "An sshj-backed control Remote. Experimental; I'm considering replacing
  jepsen.control's use of clj-ssh with this instead."
  (:require [byte-streams :as bs]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [util :as util]]
            [jepsen.control [core :as core]
                            [retry :as retry]
                            [scp :as scp]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.jcraft.jsch.agentproxy AgentProxy
                                       ConnectorFactory)
           (com.jcraft.jsch.agentproxy.sshj AuthAgent)
           (net.schmizz.sshj SSHClient)
           (net.schmizz.sshj.common IOUtils
                                    Message
                                    SSHPacket)
           (net.schmizz.sshj.connection ConnectionException)
           (net.schmizz.sshj.connection.channel OpenFailException)
           (net.schmizz.sshj.connection.channel.direct Session)
           (net.schmizz.sshj.transport.verification PromiscuousVerifier)
           (net.schmizz.sshj.userauth UserAuthException)
           (net.schmizz.sshj.userauth.method AuthMethod)
           (net.schmizz.sshj.xfer FileSystemFile
                                  LocalDestFile)
           (java.io IOException
                    InterruptedIOException)
           (java.util.concurrent Semaphore
                                 TimeUnit)))

(defn ^Iterable auth-methods
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
  [^SSHClient c {:keys [username password private-key-path] :as conn-spec}]
  (or ; Try given key
      (when-let [k private-key-path]
        (.authPublickey c ^String username
                        ^"[Ljava.lang.String;" (into-array String [k]))
        true)

      ; Try agent
      (try
        (let [agent-proxy (agent-proxy)
              methods (auth-methods agent-proxy)]
          (.auth c ^String username methods)
          true)
        (catch UserAuthException e
          false))

      ; Fall back to standard id_rsa/id_dsa keys
      (try (.authPublickey c ^String username)
           true
           (catch UserAuthException e
             false))

      ; OK, standard keys didn't work, try username+password
      (.authPassword c ^String username ^String password)))

(defn send-eof!
  "There's a bug in SSHJ where it doesn't send an EOF when you close the
  session's outputstream, which causes the remote command to hang indefinitely.
  To work around this, we send an EOF message ourselves. I'm not at all sure
  this is threadsafe; it might cause issues later."
  [^SSHClient client, ^Session session]
  (.. client
      getTransport
      (write (.. (SSHPacket. Message/CHANNEL_EOF)
                 (putUInt32 (.getRecipient session))))))

(defn handle-error
  "Takes an connection, a context map, and an SSHJ exception. Throws if it was
  caused by an InterruptedException or InterruptedIOException. Otherwise, wraps
  it in a :ssh-failed exception map, and throws that."
  [conn context e]
  ; SSHJ wraps InterruptedExceptions in its own exceptions, and we need
  ; those to bubble up properly.
  (let [cause (util/ex-root-cause e)]
    (when (or (instance? InterruptedException cause)
              (instance? InterruptedIOException cause))
      (throw cause)))
  (throw+ (merge conn context {:type :jepsen.control/ssh-failed})
          e))

(defmacro with-errors
  "Takes a conn spec, a context map, and a body. Evals body, remapping SSHJ
  exceptions to :type :jepsen.control/ssh-failed."
  [conn context & body]
  `(try
     ~@body
     ; This gets thrown when the underlying transport is not connected during
     ; session initiation
     (catch IllegalStateException e# (handle-error ~conn ~context e#))
     (catch ConnectionException   e# (handle-error ~conn ~context e#))
     (catch OpenFailException     e# (handle-error ~conn ~context e#))))

(defrecord SSHJRemote [concurrency-limit
                       conn-spec
                       ^SSHClient client
                       ^Semaphore semaphore]
  core/Remote
  (connect [this conn-spec]
    (if (:dummy conn-spec)
      (assoc this :conn-spec conn-spec)
      (try+ (let [c (as-> (SSHClient.) client
                      (do
                        (if (:strict-host-key-checking conn-spec)
                          (.loadKnownHosts client)
                          (.addHostKeyVerifier client (PromiscuousVerifier.)))
                        (.connect client
                                  ^String (:host conn-spec)
                                  (int (:port conn-spec)))
                        (auth! client conn-spec)
                        client))]
              (assoc this
                     :conn-spec conn-spec
                     :client c
                     :semaphore (Semaphore. concurrency-limit true)))
            (catch Exception e
              ; SSHJ wraps InterruptedException in its own exceptions, so we
              ; have to see through that and rethrow properly.
              (let [cause (util/ex-root-cause e)]
                (when (instance? InterruptedException cause)
                  (throw cause)))
              (throw+ (assoc conn-spec
                             :type    :jepsen.control/session-error
                             :message "Error opening SSH session. Verify username, password, and node hostnames are correct."))))))

  (disconnect! [this]
    (when-let [c client]
      (.close c)))

  (execute! [this ctx action]
    ;  (info :permits (.availablePermits semaphore))
    (when (:dummy conn-spec)
      (throw+ {:type :jepsen.control/dummy}))
    (.acquire semaphore)
    (with-errors conn-spec ctx
      (try
        (with-open [session (.startSession client)]
          (let [cmd (.exec session (:cmd action))
                ; Feed it input
                _ (when-let [input (:in action)]
                    (let [stream (.getOutputStream cmd)]
                      (bs/transfer input stream)
                      (send-eof! client session)
                      (.close stream)))
                ; Read output
                out (.toString (IOUtils/readFully (.getInputStream cmd)))
                err (.toString (IOUtils/readFully (.getErrorStream cmd)))
                ; Wait on command
                _ (.join cmd)]
            ; Return completion
            (assoc action
                   :out   out
                   :err   err
                   ; There's also a .getExitErrorMessage that might be
                   ; interesting here?
                   :exit  (.getExitStatus cmd))))
        (finally
          (.release semaphore)))))

  (upload! [this ctx local-paths remote-path _opts]
    (when (:dummy conn-spec)
      (throw+ {:type :jepsen.control/dummy}))
    (with-errors conn-spec ctx
      (with-open [sftp (.newSFTPClient client)]
        (.put sftp (FileSystemFile. (io/file local-paths))
              ^String remote-path))))

  (download! [this ctx remote-paths local-path _opts]
    (when (:dummy conn-spec)
      (throw+ {:type :jepsen.control/dummy}))
    (with-errors conn-spec ctx
      (let [local-file ^LocalDestFile (FileSystemFile. (io/file local-path))]
        (with-open [sftp (.newSFTPClient client)]
          (.get sftp ^String remote-paths local-file))))))

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
  (-> (SSHJRemote. concurrency-limit nil nil nil)
      ; We *can* use our own SCP, but shelling out is faster.
      scp/remote
      retry/remote))
