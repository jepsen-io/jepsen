(ns jepsen.control
  "Provides SSH control over a remote node. There's a lot of dynamically bound
  state in this namespace because we want to make it as simple as possible for
  scripts to open connections to various nodes."
  (:import java.io.File)
  (:require [clj-ssh.ssh    :as ssh]
            [jepsen.util    :as util :refer [real-pmap with-thread-name]]
            [dom-top.core :refer [with-retry]]
            [jepsen.reconnect :as rc]
            [clojure.string :as str]
            [clojure.tools.logging :refer [warn info debug error]]
            [slingshot.slingshot :refer [try+ throw+]]))

; STATE STATE STATE STATE
(def ^:dynamic *dummy*    "When true, don't actually use SSH" nil)
(def ^:dynamic *host*     "Current hostname"                nil)
(def ^:dynamic *session*  "Current clj-ssh session wrapper" nil)
(def ^:dynamic *trace*    "Shall we trace commands?"        false)
(def ^:dynamic *dir*      "Working directory"               "/")
(def ^:dynamic *sudo*     "User to sudo to"                 nil)
(def ^:dynamic *username* "Username"                        "root")
(def ^:dynamic *password* "Password (for login and sudo)"   "root")
(def ^:dynamic *port*     "SSH listening port"              22)
(def ^:dynamic *private-key-path*         "SSH identity file"     nil)
(def ^:dynamic *strict-host-key-checking* "Verify SSH host keys"  :yes)
(def ^:dynamic *retries*  "How many times to retry conns"   5)

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

(defrecord Literal [string])

(defn lit
  "A literal string to be passed, unescaped, to the shell."
  [s]
  (Literal. s))

(def |
  "A literal pipe character."
  (lit "|"))

(defn escape
  "Escapes a thing for the shell.

  Nils are empty strings.

  Literal wrappers are passed through directly.

  The special keywords :>, :>>, and :< map to their corresponding shell I/O
  redirection operators.

  Named things like keywords and symbols use their name, escaped. Strings are
  escaped like normal.

  Sequential collections and sets have each element escaped and
  space-separated."
  [s]
  (cond
    (nil? s)
    ""

    (instance? Literal s)
    (:string s)

    (#{:> :>> :<} s)
    (name s)

    (or (sequential? s) (set? s))
    (str/join " " (map escape s))

    :else
    (let [s (if (instance? clojure.lang.Named s)
              (name s)
              (str s))]
      (cond
        ; Empty string
        (= "" s)
        "\"\""

        (re-find #"[\\\$`\"\s\(\)\{\}\[\]\*\?<>&;]" s)
        (str "\""
             (str/replace s #"([\\\$`\"])" "\\\\$1")
             "\"")

        :else s))))

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

(defn throw-on-nonzero-exit
  "Throws when an SSH result has nonzero exit status."
  [{:keys [exit action] :as result}]
  (if (zero? exit)
    result
    (throw+
      (merge {:type ::nonzero-exit
              :cmd (:cmd action)}
             result)
      nil ; cause
      "Command exited with non-zero status %d on node %s:\n%s\n\nSTDIN:\n%s\n\nSTDOUT:\n%s\n\nSTDERR:\n%s"
      exit
      (:host result)
      (:cmd action)
      (:in result)
      (:out result)
      (:err result))))

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
                      :message "Unable to perform an SSH action because no SSH session for this host is available."}
                     (debug-data))))

    (rc/with-conn [s *session*]
      (assoc (ssh/ssh s action)
             :host   *host*
             :action action))
    (catch com.jcraft.jsch.JSchException e
      (if (and (pos? tries)
               (or (= "session is down" (.getMessage e))
                   (= "Packet corrupt" (.getMessage e))))
        (do (Thread/sleep (+ 1000 (rand-int 1000)))
            (retry (dec tries)))
        (throw+ (merge {:type ::ssh-failed}
                       (debug-data)))))))

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
       throw-on-nonzero-exit
       just-stdout))

(defn exec
  "Takes a shell command and arguments, runs the command, and returns stdout,
  throwing if an error occurs. Escapes all arguments."
  [& commands]
  (->> commands
       (map escape)
       (apply exec*)))

(defn scp*
  "Evaluates an SCP from the current host to the node."
  [current-path node-path]
  (warn "scp* is deprecated: use (upload current-path node-path) instead.")
  (rc/with-conn [s *session*]
    (ssh/scp-to *session* current-path node-path)))

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
      (let [local-paths (if (sequential? local-paths)
                          (map file->path local-paths)
                          (file->path local-paths))]
        (apply ssh/scp-to s local-paths remote-path remaining)
        remote-path))
    (catch com.jcraft.jsch.JSchException e
      (if (and (pos? tries)
               (or (= "session is down" (.getMessage e))
                   (= "Packet corrupt" (.getMessage e))))
        (do (Thread/sleep (+ 1000 (rand-int 1000)))
            (retry (dec tries)))
        (throw+ (merge {:type ::upload-failed}
                       (debug-data)))))))

(defn download
  "Copies remote paths to local node. Takes arguments for clj-ssh/scp-from.
  Retres failures."
  [& args]
  (with-retry [tries *retries*]
    (rc/with-conn [s *session*]
      (apply ssh/scp-from s args))
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

(defn check-name
  "Ensures a given hostname is string. Warns user if legacy behavior passes in a
  keyword host.
  TODO This can be removed when tests no tests generate keyword hosts. CLI already
       refuses keyword hostnames."
  [host]
  (when (keyword? host)
    (warn (str "DEPRECATED: Host "
               host
               " is a keyword; please provide node hostnames as strings. Support for keyword hosts will be removed in future versions of jepsen.")))
  (name host))

(defn clj-ssh-session
  "Opens a raw session to the given host."
  [host]
  (let [host  (check-name host)
        agent (ssh/ssh-agent {})
        _     (when *private-key-path*
                (ssh/add-identity agent
                                  {:private-key-path *private-key-path*}))]
    (doto (ssh/session agent
                       host
                       {:username *username*
                        :password *password*
                        :port *port*
                        :strict-host-key-checking *strict-host-key-checking*})
      (ssh/connect))))

(defn session
  "Wraps clj-ssh-session in a wrapper for reconnection."
  [host]
  (rc/open!
   (rc/wrapper {:open  (if *dummy*
                         (fn [] [:dummy host])
                         (fn [] (try+
                                  (clj-ssh-session host)
                                  (catch com.jcraft.jsch.JSchException _
                                    (throw+ (merge {:type ::session-error
                                                    :message "Error opening SSH session. Verify username, password, and node hostnames are correct."
                                                    :host host}
                                                   (debug-data)))))))
                :name  [:control host]
                :close (if *dummy*
                         identity
                         ssh/disconnect)
                :log?  true})))

(defn disconnect
  "Close a session"
  [session]
  (rc/close! session))

(defmacro with-ssh
  "Takes a map of SSH configuration and evaluates body in that scope. Catches
  JSchExceptions and re-throws with all available debugging context. Options:

  :dummy?
  :username
  :password
  :private-key-path
  :strict-host-key-checking"
  [ssh & body]
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
  this is just for the namespace dynamics state."
  [host session & body]
  `(binding [*host*    (name ~host)
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
                 (assert session (str "No session for node" (pr-str node)))
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
