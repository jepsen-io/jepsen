(ns jepsen.control.core
  "Provides the base protocol for running commands on remote nodes, as well as
  common functions for constructing and evaluating shell commands."
  (:require [clojure [string :as str]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defprotocol Remote
  "Remotes allow jepsen.control to run shell commands, upload, and download
  files. They use a *context map*, which encodes the current user, directory,
  etc:

    :dir      - The directory to execute remote commands in
    :sudo     - The user we want to execute a command as
    :password - The user's password, for sudo, if necessary."

  (connect [this conn-spec]
    "Set up the remote to work with a particular node. Returns a Remote which
    is ready to accept actions via `execute!` and `upload!` and `download!`.
    conn-spec is a map of:

     {:host
      :post
      :username
      :password
      :private-key-path
      :strict-host-key-checking}
    ")

  (disconnect! [this]
    "Disconnect a remote that has been connected to a host.")

  (execute! [this context action]
    "Execute the specified action in a remote connected a host. Takes a context
    map, and an action: a map of...

      :cmd   A string command to execute.
      :in    A string to provide for the command's stdin.

    Should return the action map with additional keys:

      :exit  The command's exit status.
      :out   The stdout string.
      :err   The stderr string.
    ")

  (upload! [this context local-paths remote-path opts]
    "Copy the specified local-path to the remote-path on the connected host.

    Opts is an option map. There are no defined options right now, but later we
    might introduce some for e.g. recursive uploads, compression, etc. This is
    also a place for Remote implementations to offer custom semantics.")

  (download! [this context remote-paths local-path opts]
    "Copy the specified remote-paths to the local-path on the connected host.

    TODO: remote-paths is, in fact, a single remote path: it looks like I
    forgot to finish making it multiple paths. May want to fix this later--not
    sure whether it should be a single path or multiple.

    Opts is an option map. There are no defined options right now, but later we
    might introduce some for e.g. recursive uploads, compression, etc. This is
    also a place for Remote implementations to offer custom semantics."))

(defrecord Literal [string])

(defn lit
  "A literal string to be passed, unescaped, to the shell."
  [s]
  (Literal. s))

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

(defn env
  "We often want to construct env vars for a process. This function takes a map
  of environment variable names (any Named type, e.g. :HOME, \"HOME\") to
  values (which are coerced using `(str value)`), and constructs a Literal
  string, suitable for passing to exec, which binds those environment
  variables.

  Callers of this function (especially indirectly, as with start-stop-daemon),
  may wish to construct env var strings themselves. Passing a string `s` to this
  function simply returns `(lit s)`. Passing a Literal `l` to this function
  returns `l`. nil is passed through unchanged."
  [env]
  (cond (map? env) (->> env
                        (map (fn [[k v]]
                               (str (name k) "=" (escape v))))
                        (str/join " ")
                        lit)

        (instance? Literal env)
        env

        (instance? String env)
        (lit env)

        (nil? env) nil

        :else
        (throw (IllegalArgumentException.
                 (str "Unsure how to construct an environment variable mapping from " (pr-str env))))))

(defn wrap-sudo
  "Takes a context map and a command action, and returns the command action,
  modified to wrap it in a sudo command, if necessary. Uses the context map's
  :sudo and :sudo-password fields."
  [{:keys [sudo sudo-password]} cmd]
  (if sudo
    (cond-> (assoc cmd :cmd (str "sudo -k -S -u " sudo " bash -c "
                                 (escape (:cmd cmd))))
      ; If we have a password, provide it in the input so sudo sees it.
      sudo-password (assoc :in (str sudo-password "\n" (:in cmd))))
    ; Not a sudo context!
    cmd))

(defn throw-on-nonzero-exit
  "Throws when an SSH result has nonzero exit status."
  [{:keys [exit action] :as result}]
  (if (zero? exit)
    result
    (throw+
      (merge {:type :jepsen.control/nonzero-exit
              :cmd (:cmd action)}
             result)
      nil ; cause
      "Command exited with non-zero status %d on node %s:\n%s\n\nSTDIN:\n%s\n\nSTDOUT:\n%s\n\nSTDERR:\n%s"
      exit
      (:host result)
      (:cmd action)
      (:in action)
      (:out result)
      (:err result))))
