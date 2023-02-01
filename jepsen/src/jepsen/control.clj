(ns jepsen.control
  "Provides control over a remote node. There's a lot of dynamically bound
  state in this namespace because we want to make it as simple as possible for
  scripts to open connections to various nodes.

  Note that a whole bunch of this namespace refers to things as 'ssh',
  although they really can apply to any remote, not just SSH."
  (:require [jepsen.util    :as util :refer [real-pmap with-thread-name]]
            [jepsen.control [clj-ssh :as clj-ssh]
                            [core :as core
                             :refer [connect
                                     disconnect!
                                     execute!
                                     upload!
                                     download!]]
                            [sshj :as sshj]]
            [potemkin :refer [import-vars]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [warn info debug error]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import java.io.File
           (java.util.concurrent Semaphore)))

; These used to be in jepsen.control, but have been moved to
; jepsen.control.core as a part of the polymorphic Remote protocol work. We
; preserve them here for backwards compatibility.
(import-vars
  [jepsen.control.core
   env
   escape
   lit
   throw-on-nonzero-exit])

(def clj-ssh
  "The clj-ssh SSH remote. This used to be the default."
  (clj-ssh/remote))

(def ssh
  "The default (SSHJ-backed) remote."
  (sshj/remote))

; STATE STATE STATE STATE
(def ^:dynamic *dummy*    "When true, don't actually use SSH" nil)
(def ^:dynamic *host*     "Current hostname"                nil)
(def ^:dynamic *session*  "Current control session wrapper" nil)
(def ^:dynamic *trace*    "Shall we trace commands?"        false)
(def ^:dynamic *dir*      "Working directory"               "/")
(def ^:dynamic *sudo*     "User to sudo to"                 nil)
(def ^:dynamic *sudo-password* "Password for sudo, if needed" nil)
(def ^:dynamic *username* "Username"                        "root")
(def ^:dynamic *password* "Password (for login)"            "root")
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
  {:dir           *dir*
   :sudo          *sudo*
   :sudo-password *sudo-password*})

(defn debug-data
  "Construct a map of SSH data for debugging purposes."
  []
  {:dummy                    *dummy*
   :host                     *host*
   :session                  *session*
   :dir                      *dir*
   :sudo                     *sudo*
   :sudo-password            *sudo-password*
   :username                 *username*
   :password                 *password*
   :port                     *port*
   :private-key-path         *private-key-path*
   :strict-host-key-checking *strict-host-key-checking*})

(def |
  "A literal pipe character."
  (lit "|"))

(def &&
  "A literal &&"
  (lit "&&"))

(defn wrap-sudo
  "Wraps command in a sudo subshell."
  [cmd]
  (core/wrap-sudo (cmd-context) cmd))

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
  (when (nil? *session*)
    (throw+ (merge {:type ::no-session-available
                    :message "Unable to perform a control action because no session for this host is available."}
                   (debug-data))))
  (assoc (execute! *session* (cmd-context) action)
         :host   *host*
         :action action))

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
       core/throw-on-nonzero-exit
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
    (.getCanonicalPath ^File x)
    x))

(defn upload
  "Copies local path(s) to remote node and returns the remote path."
  [local-paths remote-path]
  (let [s *session*
        local-paths (map file->path (util/coll local-paths))]
    (upload! s (cmd-context) local-paths remote-path {})
    remote-path))

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
  "Copies remote paths to local node."
  [remote-paths local-path]
  (download! *session* (cmd-context) remote-paths local-path {}))

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
  "Returns a Remote bound to the given host."
  [host]
  (connect *remote* (assoc (conn-spec) :host host)))

(defn disconnect
  "Close a Remote session."
  [remote]
  (core/disconnect! remote))

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
    :sudo-password
    :private-key-path
    :strict-host-key-checking"
  [ssh & body]
  ; TODO: move this into a single *conn-spec* variable. Some external code
  ; reads *host*, so we're not doing this just yet.
  `(binding [*dummy*            (get ~ssh :dummy?           *dummy*)
             *username*         (get ~ssh :username         *username*)
             *sudo-password*    (get ~ssh :sudo-password    *sudo-password*)
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
