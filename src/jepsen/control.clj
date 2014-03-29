(ns jepsen.control
  "Provides SSH control over a remote node. There's a lot of dynamically bound
  state in this namespace because we want to make it as simple as possible for
  scripts to open connections to various nodes."
  (:require [clj-ssh.ssh :as ssh]
            [clojure.string :as str]))

(def ^:dynamic *host*     "Current hostname"              nil)
(def ^:dynamic *session*  "Current clj-ssh session"       nil)
(def ^:dynamic *trace*    "Shall we trace commands?"      false)
(def ^:dynamic *dir*      "Working directory"             "/")
(def ^:dynamic *sudo*     "User to sudo to"               nil)
(def ^:dynamic *username* "Username"                      "ubuntu")
(def ^:dynamic *password* "Password (for login and sudo)" "ubuntu")
(def ^:dynamic *private-key-path*         "SSH identity file"     nil)
(def ^:dynamic *strict-host-key-checking* "Verify SSH host keys"  :yes)

(defn escape
  "Escapes a shell string."
  [s]
  (if (nil? s)
    ""
    (let [s (if (instance? clojure.lang.Named s)
              (name s)
              (str s))]
      (if (re-find #"[\\\$`\" \(\)\{\}\[\]]" s)
        (str "\""
             (str/replace s #"([\\\$`\"])" "\\\\$1")
             "\"")
        s))))

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
  (do (when *trace* (prn arg))
      arg))

(defn throw-on-nonzero-exit
  "Throws when the result of an SSH result has nonzero exit status."
  [result]
  (if (zero? (:exit result))
    result
    (throw (RuntimeException. (:err result)))))

(defn just-stdout
  "Returns the stdout from an ssh result, trimming any newlines at the end."
  [result]
  (str/trim-newline (:out result)))

(defn ssh*
  "Evaluates an SSH action against the current host."
  [action]
  (ssh/ssh *session* action))

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
  "Opens a session to the given host."
  [host]
  (let [host  (name host)
        agent (ssh/ssh-agent {})
        _     (when *private-key-path*
                (ssh/add-identity agent
                                  {:private-key-path *private-key-path*}))]
    (doto (ssh/session agent
                       host
                       {:username *username*
                        :password *password*
                        :strict-host-key-checking *strict-host-key-checking*})
      (ssh/connect))))

(def disconnect
  "Close a session"
  ssh/disconnect)

(defmacro with-ssh
  "Takes a map of SSH configuration and evaluates body in that scope. Options:

  :username
  :password
  :private-key-path
  :strict-host-key-checking"
  [ssh & body]
  `(binding [*username*         (get ~ssh :username *username*)
             *password*         (get ~ssh :password *password*)
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
     (ssh/with-connection session#
       (with-session ~host session#
         ~@body))))

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

(defn go
  [host]
  (on host
      (trace
        (cd "/"
            (sudo "root"
                  (println (exec "whoami")))))))
