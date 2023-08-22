(ns jepsen.control.util
  "Utility functions for scripting installations."
  (:require [jepsen.control :refer :all]
            [jepsen.control.core :as core]
            [jepsen.util :as util :refer [meh name+ timeout]]
            [clojure.data.codec.base64 :as b64]
            [clojure.java.io :refer [file]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [slingshot.slingshot :refer [try+ throw+]]))

(def tmp-dir-base "Where should we put temporary files?" "/tmp/jepsen")

(defn await-tcp-port
  "Blocks until a local TCP port is bound. Options:

  :retry-interval   How long between retries, in ms. Default 1s.
  :log-interval     How long between logging that we're still waiting, in ms.
                    Default `retry-interval.
  :timeout          How long until giving up and throwing :type :timeout, in
                    ms. Default 60 seconds."
  ([port]
   (await-tcp-port port {}))
  ([port opts]
   (util/await-fn
     (fn check-port []
       (exec :nc :-z :localhost port)
       nil)
     (merge {:log-message (str "Waiting for port " port " ...")}
            opts))))

(defn file?
  "Is `filename` a regular file that exists?"
  [filename]
  (try+
   (exec :test :-f filename)
   true
   (catch [:exit 1] _
     false)))

(defn exists?
  "Is a path present?"
  [filename]
  (try (exec :stat filename)
       true
       (catch RuntimeException _ false)))

(defn ls
  "A seq of directory entries (not including . and ..). TODO: escaping for
  control chars in filenames (if you do this, WHO ARE YOU???)"
  ([] (ls "."))
  ([dir]
   (->> (str/split (exec :ls :-A dir) #"\n")
        (remove str/blank?))))

(defn ls-full
  "Like ls, but prepends dir to each entry."
  [dir]
  (let [dir (if (re-find #"/$" dir)
              dir
              (str dir "/"))]
    (->> dir
         ls
         (map (partial str dir)))))

(defn tmp-file!
  "Creates a random, temporary file under tmp-dir-base, and returns its path."
  []
  (let [file (str tmp-dir-base "/" (rand-int Integer/MAX_VALUE))]
    (if (exists? file)
      (recur)
      (do
        (try+
          (exec :touch file)
          (catch [:exit 1] _
            ; Parent dir might not exist
            (exec :mkdir :-p tmp-dir-base)
            (exec :touch file)))
        file))))

(defn tmp-dir!
  "Creates a temporary directory under /tmp/jepsen and returns its path."
  []
  (let [dir (str tmp-dir-base "/" (rand-int Integer/MAX_VALUE))]
    (if (exists? dir)
      (recur)
      (do
        (exec :mkdir :-p dir)
        dir))))

(defn write-file!
  "Writes a string to a filename."
  [string file]
  (let [cmd (->> [:cat :> file]
                 (map escape)
                 (str/join " "))
        action {:cmd cmd
                :in  string}]
    (-> action
        wrap-cd
        wrap-sudo
        wrap-trace
        ssh*
        core/throw-on-nonzero-exit)
    file))

(def std-wget-opts
  "A list of standard options we pass to wget"
  [:--tries 20
   :--waitretry 60
   :--retry-connrefused
   :--dns-timeout 60
   :--connect-timeout 60
   :--read-timeout 60])

(defn wget-helper!
  "A helper for wget! and cached-wget!. Calls wget with options; catches name
  resolution and other network errors, and retries them. EC2 name resolution
  can be surprisingly flaky."
  [& args]
  (loop [tries 5]
    (let [res (try+
                (exec :wget args)
                (catch [:type :jepsen.control/nonzero-exit, :exit 4] e
                  (if (pos? tries)
                    ::retry
                    (throw+ e))))]
      (if (= ::retry res)
        (recur (dec tries))
        res))))


; TODO: only force? should have a ?, because it's a boolean. User and pw
; should be renamed without ?, and probably use whatever username/password
; naming convention we use in jepsen.control etc.
(defn wget!
  "Downloads a string URL and returns the filename as a string. Skips if the
  file already exists.

  Options:

    :force?      Even if we have this cached, download the tarball again anyway.
    :user?       User for wget authentication. If provided, valid pw must also be provided.
    :pw?         Password for wget authentication."
  ([url]
   (wget! url {:force? false}))
  ([url opts]
   (let [filename (.getName (file url))
         wget-opts std-wget-opts
         ; second parameter was changed from a boolean flag (force?) to an
         ; options map this check is here for backwards compatibility
         opts (if (map? opts) opts {:force? opts})]
     (when (:force? opts)
       (exec :rm :-f filename))
     (when-not (empty? (:user? opts))
       (concat wget-opts [:--user (:user? opts) :--password (:pw? opts)]))
     (when (not (exists? filename))
       (wget-helper! wget-opts url))
     filename)))

(def wget-cache-dir
  "Directory for caching files from the web."
  (str tmp-dir-base "/wget-cache"))

(defn encode
  "base64 encode a given string and return the encoded string in utf8"
  [^String s]
  (String. ^bytes (b64/encode (.getBytes s)) "UTF-8"))

(defn cached-wget!
  "Downloads a string URL to the Jepsen wget cache directory, and returns the
  full local filename as a string. Skips if the file already exists. Local
  filenames are base64-encoded URLs, as opposed to the name of the file--this
  is helpful when you want to download a package like
  https://foo.com/v1.2/foo.tar; since the version is in the URL but not a part
  of the filename, downloading a new version could silently give you the old
  version instead.

  Options:

    :force?      Even if we have this cached, download the tarball again anyway.
    :user?       User for wget authentication. If provided, valid pw must also be provided.
    :pw?         Password for wget authentication."
  ([url]
   (cached-wget! url {:force? false}))
  ([url opts]
   (let [encoded-url (encode url)
         dest-file   (str wget-cache-dir "/" encoded-url)
         wget-opts   (if (empty? (:user? opts))
                       (concat std-wget-opts [:-O dest-file])
                       (concat std-wget-opts [:-O dest-file :--user (:user? opts) :--password (:pw? opts)]))]
     (when (:force? opts)
       (info "Clearing cached copy of" url)
       (exec :rm :-rf dest-file))
     (when-not (exists? dest-file)
       (info "Downloading" url)
       (do (exec :mkdir :-p wget-cache-dir)
           (cd wget-cache-dir
               (wget-helper! wget-opts url))))
     dest-file)))

(defn install-archive!
  "Gets the given tarball URL, caching it in /tmp/jepsen/, and extracts its
  sole top-level directory to the given dest directory. Deletes
  current contents of dest. Supports both zip files and tarballs, compressed or
  raw. Returns dest.

  Standard practice for release tarballs is to include a single directory,
  often named something like foolib-1.2.3-amd64, with files inside it. If only
  a single directory is present, its *contents* will be moved to dest, so
  foolib-1.2.3-amd64/my.file becomes dest/my.file. If the tarball includes
  multiple files, those files are moved to dest, so my.file becomes
  dest/my.file.

  Options:

    :force?      Even if we have this cached, download the tarball again anyway.
    :user?       User for wget authentication. If provided, valid pw must also be provided.
    :pw?         Password for wget authentication."
  ([url dest]
   (install-archive! url dest {:force? false}))
  ([url dest opts]
   (let [local-file (nth (re-find #"file://(.+)" url) 1)
         file       (or local-file (cached-wget! url opts))
         tmpdir     (tmp-dir!)
         dest       (expand-path dest)]

     ; Clean up old dest and make sure parent directory is ready
     (exec :rm :-rf dest)
     (let [parent (exec :dirname dest)]
       (exec :mkdir :-p parent))

     (try+
       (cd tmpdir
           ; Extract archive to tmpdir
           (if (re-find #".*\.zip$" url)
             (exec :unzip file)
             (exec :tar :--no-same-owner :--no-same-permissions
                   :--extract :--file file))

           ; Force ownership
           (when (= "root" *sudo*)
             (exec :chown :-R "root:root" "."))

           ; Get archive root paths
           (let [roots (ls)]
             (assert (pos? (count roots)) "Archive contained no files")

             (if (= 1 (count roots))
               ; Move root's contents to dest
               (exec :mv (first roots) dest)

               ; Move all roots to dest
               (exec :mv tmpdir dest))))

       (catch [:type :jepsen.control/nonzero-exit] e
         (let [err (:err e)]
           (if (or (re-find #"tar: Unexpected EOF" err)
                   (re-find #"This does not look like a tar archive" err)
                   (re-find #"cannot find zipfile directory" err))
             (if local-file
               ; Nothing we can do to recover here
               (throw (RuntimeException.
                        (str "Local archive " local-file " on node "
                             *host*
                             " is corrupt: " err)))
               ; Retry download once; maybe it was abnormally terminated
               (do (info "Retrying corrupt archive download")
                   (exec :rm :-rf file)
                   (install-archive! url dest opts)))

             ; Throw by default
             (throw+ e))))

       (finally
         ; Clean up tmpdir
         (exec :rm :-rf tmpdir))))
   dest))

(defn ensure-user!
  "Make sure a user exists."
  [username]
  (try (su (exec :adduser :--disabled-password :--gecos (lit "''") username))
       (catch RuntimeException e
         (when-not (re-find #"already exists" (.getMessage e))
           (throw e))))
  username)

(defn grepkill!
  "Kills processes by grepping for the given string. If a signal is given,
  sends that signal instead. Signals may be either numbers or names, e.g.
  :term, :hup, ..."
  ([pattern]
   (grepkill! 9 pattern))
  ([signal pattern]
   ; Hahaha we'd like to use pkill here, but because we run sudo commands in a
   ; bash wrapper (`bash -c "pkill ..."`), we'd end up matching the bash wrapper
   ; and killing that as WELL, so... grep and awk it is! The grep -v makes sure
   ; we don't kill the grep process OR the bash process executing it.
   (try+ (exec ;:ps :aux
               ;| :grep pattern
               ;| :grep :-v "grep"
               ;| :awk "{print $2}"
               :pgrep pattern
               | :xargs :--no-run-if-empty :kill (str "-" (name+ signal)))
         (catch [:type :jepsen.control/nonzero-exit, :exit 0] _
           nil)
         (catch [:type :jepsen.control/nonzero-exit, :exit 123] e
           (if (re-find #"No such process" (:err e))
             ; Ah, process already exited
             nil
             (throw+ e))))))

(defn start-daemon!
  "Starts a daemon process, logging stdout and stderr to the given file.
  Invokes `bin` with `args`. Options are:

  :env                  Environment variables for the invocation of
                        start-stop-daemon. Should be a Map of env var names to
                        string values, like {:SEEDS \"flax, cornflower\"}. See
                        jepsen.control/env for alternative forms.
  :background?
  :chdir
  :exec                 Sets a custom executable to check for.
  :logfile
  :make-pidfile?
  :match-executable?    Helpful for cases where the daemon is a wrapper script
                        that execs another process, so that pidfile management
                        doesn't work right. When this option is true, we ask
                        start-stop-daemon to check for any process running the
                        given executable program: either :exec or the `bin`
                        argument.
  :match-process-name?  Helpful for cases where the daemon is a wrapper script
                        that execs another process, so that pidfile management
                        doesn't work right. When this option is true, we ask
                        start-stop-daemon to check for any process with a COMM
                        field matching :process-name (or the name of the bin).
  :pidfile              Where should we write (and check for) the pidfile? If
                        nil, doesn't use the pidfile at all.
  :process-name         Overrides the process name for :match-process-name?

  Returns :started if the daemon was started, or :already-running if it was
  already running, or throws otherwise."
  [opts bin & args]
  (let [env  (env (:env opts))
        ssd-args [:--start
                  (when (:background? opts true) [:--background :--no-close])
                  (when (and (:pidfile opts) (:make-pidfile? opts true))
                    :--make-pidfile)
                  (when (:match-executable? opts true)
                    [:--exec (or (:exec opts) bin)])
                  (when (:match-process-name? opts false)
                    [:--name (:process-name opts (.getName (file bin)))])
                  (when (:pidfile opts)
                    [:--pidfile (:pidfile opts)])
                  :--chdir    (:chdir opts)
                  :--startas  bin
                  :--
                  args
                  :>> (:logfile opts) (lit "2>&1")]]
    (info "Starting" (.getName (file (name bin))))
    (exec :echo (lit "`date +'%Y-%m-%d %H:%M:%S'`")
          (str "Jepsen starting " (escape env) " " bin " " (escape args))
          :>> (:logfile opts))
    (try+
      ;(info "start-stop-daemon" (escape ssd-args))
      (exec env :start-stop-daemon ssd-args)
      :started
      (catch [:type   :jepsen.control/nonzero-exit
              :exit   1] e
        :already-running))))

(defn stop-daemon!
  "Kills a daemon process by pidfile, or, if given a command name, kills all
  processes with that command name, and cleans up pidfile. Pidfile may be nil
  in the two-argument case, in which case it is ignored."
  ([pidfile]
   (when (exists? pidfile)
     (info "Stopping" pidfile)
     (let [pid (Long/parseLong (exec :cat pidfile))]
       (meh (exec :kill :-9 pid))
       (meh (exec :rm :-rf pidfile)))))

  ([cmd pidfile]
   (info "Stopping" cmd)
   (timeout 30000 (throw+ {:type    ::kill-timed-out
                           :cmd     cmd
                           :pidfile pidfile})
            (meh (exec :killall :-9 :-w cmd)))
   (when pidfile
     (meh (exec :rm :-rf pidfile)))))

(defn daemon-running?
  "Given a pidfile, returns true if the pidfile is present and the process it
  contains is alive, nil if the pidfile is absent, false if it's present and
  the process doesn't exist.

  Strictly this doesn't mean the process is RUNNING; it could be asleep or a
  zombie, but you know what I mean. ;-)"
  [pidfile]
  (when-let [pid (meh (exec :cat pidfile))]
    (try (exec :ps :-o "pid=" :-p pid)
         (catch RuntimeException e
           false))))

(defn signal!
  "Sends a signal to a named process by signal number or name."
  [process-name signal]
  (meh (exec :pkill :--signal signal process-name))
  :signaled)
