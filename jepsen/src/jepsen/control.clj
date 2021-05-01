(ns jepsen.control
  "Provides control over a remote node. There's a lot of dynamically bound
  state in this namespace because we want to make it as simple as possible for
  scripts to open connections to various nodes.

  Note that a whole bunch of this namespace refers to things as 'ssh',
  although they really can apply to any remote, not just SSH."
  (:require [clj-ssh.ssh    :as ssh]
            [jepsen.util    :as util :refer [real-pmap with-thread-name]]
            [dom-top.core :refer [with-retry]]
            [jepsen.reconnect :as rc]
            [jepsen.control [clj-ssh :as clj-ssh]
                            [remote :as remote
                             :refer [connect
                                     disconnect!
                                     execute!
                                     upload!
                                     download!]]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [warn info debug error]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import java.io.File
           (java.util.concurrent Semaphore)))

(def ssh
  "The default SSH remote"
  (clj-ssh/remote))

; STATE STATE STATE STATE
(def ^:dynamic *dummy*    "When true, don't actually use SSH" nil)
(def ^:dynamic *host*     "Current hostname"                nil)
(def ^:dynamic *session*  "Current control session wrapper" nil)
(def ^:dynamic *trace*    "Shall we trace commands?"        false)
(def ^:dynamic *dir*      "Working directory"               "/")
(def ^:dynamic *sudo*     "User to sudo to"                 nil)
(def ^:dynamic *username* "Username"                        "root")
(def ^:dynamic *password* "Password (for login and sudo)"   "root")
(def ^:dynamic *port*     "SSH listening port"              22)
(def ^:dynamic *private-key-path*         "SSH identity file"     nil)
(def ^:dynamic *strict-host-key-checking* "Verify SSH host keys"  :yes)
(def ^:dynamic *remote*   "The remote to use for remote control actions" ssh)
(def ^:dynamic *retries*  "How many times to retry conns"   5)

(defn conn-spec
  "jepsen.control originally stored everything--host, post, etc.--in separate
  dynamic variables. Now, we store these things in a conn-spec map, which can
  be passed to remotes without creating cyclic dependencies. This function
  exists to support the transition from those variables to a conn-spec, and
  constructs a conn spec from current var bindings."
  []
  {; TODO: pull this out of conn-spec and the clj-ssh remote, and create
   ; a specialized remote for it.
   :dummy                    *dummy*
   :host                     *host*
   :port                     *port*
   :username                 *username*
   :password                 *password*
   :private-key-path         *private-key-path*
   :strict-host-key-checking *strict-host-key-checking*})

(defn cmd-context
  "Constructs a context map for a command's execution from dynamically bound
  vars."
  []
  {:dir  *dir*
   :sudo *sudo*})

(defn debug-data
  "Construct a map of SSH data for debugging purposes."
  []
  {:dummy                    *dummy*
   :host                     *host*
   :session                  *session*
   :dir                      *dir*
   :sudo                     *sudo*
   :username                 *username*
   :password                 *password*
   :port                     *port*
   :private-key-path         *private-key-path*
   :strict-host-key-checking *strict-host-key-checking*})

(def lit remote/lit)

(def |
  "A literal pipe character."
  (lit "|"))

(def &&
  "A literal &&"
  (lit "&&"))

(def escape remote/escape)

(def throw-on-nonzero-exit remote/throw-on-nonzero-exit)

(defn wrap-sudo
  "Wraps command in a sudo subshell."
  [cmd]
  (if *sudo*
    (merge cmd {:cmd (str "sudo -S -u " *sudo* " bash -c " (escape (:cmd cmd)))
                :in  (if *password*
                       (str *password* "\n" (:in cmd))
                       (:in cmd))})
    cmd))

(defn wrap-cd
  "Wraps command by changing to the current bound directory first."
  [cmd]
  (if *dir*
    (assoc cmd :cmd (str "cd " (escape *dir*) "; " (:cmd cmd)))
    cmd))

(defn wrap-trace
  "Logs argument to console when tracing is enabled."
  [arg]
  (do (when *trace* (info "Host:" *host* "arg:" arg))
      arg))

(defn just-stdout
  "Returns the stdout from an ssh result, trimming any newlines at the end."
  [result]
  (str/trim-newline (:out result)))

(defn ssh*
  "Evaluates an SSH action against the current host. Retries packet corrupt
  errors."
  [action]
  (with-retry [tries *retries*]
    (when (nil? *session*)
      (throw+ (merge {:type ::no-session-available
                      :message "Unable to perform a control action because no session for this host is available."}
                     (debug-data))))

    (rc/with-conn [s *session*]
      (assoc (execute! s (cmd-context) action)
             :host   *host*
             :action action))
    (catch com.jcraft.jsch.JSchException e
      (if (and (pos? tries)
               (or (= "session is down" (.getMessage e))
                   (= "Packet corrupt" (.getMessage e))))
        (do (Thread/sleep (+ 1000 (rand-int 1000)))
            (retry (dec tries)))
        (throw+ (merge {:type ::ssh-failed}
                       (debug-data))
                e)))))

(defn exec*
  "Like exec, but does not escape."
  [& commands]
  (->> commands
       (str/join " ")
       (array-map :cmd)
       wrap-cd
       wrap-sudo
       wrap-trace
       ssh*
       remote/throw-on-nonzero-exit
       just-stdout))

(defn exec
  "Takes a shell command and arguments, runs the command, and returns stdout,
  throwing if an error occurs. Escapes all arguments."
  [& commands]
  (->> commands
       (map escape)
       (apply exec*)))

(defn file->path
  "Takes an object, if it's an instance of java.io.File, gets the path, otherwise
  returns the object"
  [x]
  (if (instance? java.io.File x)
    (.getCanonicalPath x)
    x))

(defn upload
  "Copies local path(s) to remote node and returns the remote path.
  Takes arguments for clj-ssh/scp-to."
  [& [local-paths remote-path & remaining]]
  (with-retry [tries *retries*]
    (rc/with-conn [s *session*]
      (let [local-paths (map file->path (util/coll local-paths))]
        (upload! s (cmd-context) local-paths remote-path remaining)
        remote-path))
    (catch com.jcraft.jsch.JSchException e
      (if (and (pos? tries)
               (or (= "session is down" (.getMessage e))
                   (= "Packet corrupt" (.getMessage e))))
        (do (Thread/sleep (+ 1000 (rand-int 1000)))
            (retry (dec tries)))
        (throw+ (merge {:type ::upload-failed}
                       (debug-data)))))))

(defn upload-resource!
  "Uploads a local JVM resource (as a string) to the given remote path."
  [resource-name remote-path]
  (with-open [reader (io/reader (io/resource resource-name))]
    (let [tmp-file (File/createTempFile "jepsen-resource" ".upload")]
      (try
        (io/copy reader tmp-file)
        (upload (.getCanonicalPath tmp-file) remote-path)
        (finally
          (.delete tmp-file))))))

(defn download
  "Copies remote paths to local node. Takes arguments for clj-ssh/scp-from.
  Retres failures."
  [& [remote-paths local-path & remaining]]
  (with-retry [tries *retries*]
    (rc/with-conn [s *session*]
      (download! s (cmd-context) remote-paths local-path remaining))
    (catch clojure.lang.ExceptionInfo e
      (if (and (pos? tries)
               (re-find #"disconnect error" (.getMessage e)))
        (do (Thread/sleep (+ 1000 (rand-int 1000)))
            (retry (dec tries)))
        (throw+ (assoc (debug-data)
                       :type ::download-failed))))
    (catch com.jcraft.jsch.JSchException e
      (if (and (pos? tries)
               (or (= "session is down" (.getMessage e))
                   (= "Packet corrupt" (.getMessage e))))
        (do (Thread/sleep (+ 1000 (rand-int 1000)))
            (retry (dec tries)))
        (throw+ (merge {:type ::download-failed}
                       (debug-data)))))))

(defn expand-path
  "Expands path relative to the current directory."
  [path]
  (if (re-find #"^/" path)
    ; Absolute
    path
    ; Relative
    (str *dir* (if (re-find #"/$" path)
                 ""
                 "/")
         path)))

(defmacro cd
  "Evaluates forms in the given directory."
  [dir & body]
  `(binding [*dir* (expand-path ~dir)]
     ~@body))

(defmacro sudo
  "Evaluates forms with a particular user."
  [user & body]
  `(binding [*sudo* (name ~user)]
     ~@body))

(defmacro su
  "sudo root ..."
  [& body]
  `(sudo :root ~@body))

(defmacro trace
  "Evaluates forms with command tracing enabled."
  [& body]
  `(binding [*trace* true]
     ~@body))

(defn session
  "Wraps control session in a wrapper for reconnection."
  [host]
  (let [remote *remote*
        conn-spec (assoc (conn-spec) :host host)]
    (rc/open!
      (rc/wrapper {:open  (fn [] (connect remote conn-spec))
                   :name  [:control host]
                   :close disconnect!
                   :log?  true}))))

(defn disconnect
  "Close a session"
  [session]
  (rc/close! session))

(defmacro with-remote
  "Takes a remote and evaluates body with that remote in that scope."
  [remote & body]
  `(binding [*remote* ~remote] ~@body))

(defmacro with-ssh
  "Takes a map of SSH configuration and evaluates body in that scope. Catches
  JSchExceptions and re-throws with all available debugging context. Options:

  :dummy?
  :username
  :password
  :private-key-path
  :strict-host-key-checking"
  [ssh & body]
  ; TODO: move this into a single *conn-spec* variable. Some external code
  ; reads *host*, so we're not doing this just yet.
  `(binding [*dummy*            (get ~ssh :dummy?           *dummy*)
             *username*         (get ~ssh :username         *username*)
             *password*         (get ~ssh :password         *password*)
             *port*             (get ~ssh :port             *port*)
             *private-key-path* (get ~ssh :private-key-path *private-key-path*)
             *strict-host-key-checking* (get ~ssh :strict-host-key-checking
                                             *strict-host-key-checking*)]
     ~@body))

(defmacro with-session
  "Binds a host and session and evaluates body. Does not open or close session;
  this is just for the namespace dynamic state."
  [host session & body]
  `(binding [*host*    ~host
             *session* ~session]
     ~@body))

(defmacro on
  "Opens a session to the given host and evaluates body there; and closes
  session when body completes."
  [host & body]
  `(let [session# (session ~host)]
     (try+
       (with-session ~host session#
         ~@body)
       (finally
         (disconnect session#)))))

(defmacro on-many
  "Takes a list of hosts, executes body on each host in parallel, and returns a
  map of hosts to return values."
  [hosts & body]
  `(let [hosts# ~hosts]
     (->> hosts#
          (map #(future (on % ~@body)))
          doall
          (map deref)
          (map vector hosts#)
          (into {}))))

(defn on-nodes
  "Given a test, evaluates (f test node) in parallel on each node, with that
  node's SSH connection bound. If `nodes` is provided, evaluates only on those
  nodes in particular."
  ([test f]
   (on-nodes test (:nodes test) f))
  ([test nodes f]
   (->> nodes
        (map (fn [node]
               (let [session (get (:sessions test) node)]
                 (assert session (str "No session for node " (pr-str node)))
                 [node session])))
        (real-pmap (bound-fn [[node session]]
                     (with-thread-name (str "jepsen node " (name node))
                       (with-session node session
                         [node (f test node)]))))
        (into {}))))

(defmacro with-test-nodes
  "Given a test, evaluates body in parallel on each node, with that node's SSH
  connection bound."
  [test & body]
  `(on-nodes ~test
             (fn [test# node#]
               ~@body)))
