(ns jepsen.control.util
  "Utility functions for scripting installations."
  (:require [jepsen.control :refer :all]
            [jepsen.util :refer [meh]]
            [clojure.java.io :refer [file]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]))

(def tmp-dir-base "Where should we put temporary files?" "/tmp/jepsen")

(defn file?
  [filename]
  (throw (RuntimeException. "Use exists? instead; file? will be used to tell if
                            something is a file, as opposed to a directory or
                            link.")))

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

(defn tmp-dir!
  "Creates a temporary directory under /tmp/jepsen and returns its path."
  []
  (let [dir (str tmp-dir-base "/" (rand-int Integer/MAX_VALUE))]
    (if (exists? dir)
      (recur)
      (do
        (exec :mkdir :-p dir)
        dir))))

(defn wget!
  "Downloads a string URL and returns the filename as a string. Skips if the
  file already exists."
  ([url]
   (wget! url false))
  ([url force?]
   (let [filename (.getName (file url))]
     (when force?
       (exec :rm :-f filename))
     (when (not (exists? filename))
       (exec :wget
             :--tries 20
             :--waitretry 60
             :--retry-connrefused
             :--dns-timeout 60
             :--connect-timeout 60
             :--read-timeout 60
             url))
     filename)))

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
  dest/my.file."
  ([url dest]
   (install-archive! url dest false))
  ([url dest force?]
   (let [local-file (nth (re-find #"file://(.+)" url) 1)
         file       (or local-file
                        (do (exec :mkdir :-p tmp-dir-base)
                            (cd tmp-dir-base
                                (expand-path (wget! url force?)))))
         tmpdir     (tmp-dir!)
         dest       (expand-path dest)]

     ; Clean up old dest and make sure parent directory is ready
     (exec :rm :-rf dest)
     (let [parent (exec :dirname dest)]
       (exec :mkdir :-p parent))

     (try
       (cd tmpdir
           ; Extract archive to tmpdir
           (if (re-find #".*\.zip$" file)
             (exec :unzip file)
             (exec :tar :xf file))

           ; Get archive root paths
           (let [roots (ls)]
             (assert (pos? (count roots)) "Archive contained no files")

             (if (= 1 (count roots))
               ; Move root's contents to dest
               (exec :mv (first roots) dest)

               ; Move all roots to dest
               (exec :mv tmpdir dest))))

       (catch RuntimeException e
         (condp re-find (.getMessage e)
           #"tar: Unexpected EOF"
           (if local-file
             ; Nothing we can do to recover here
             (throw (RuntimeException.
                      (str "Local archive " local-file " on node "
                           *host*
                           " is corrupt: unexpected EOF.")))
             (do (info "Retrying corrupt archive download")
                 (exec :rm :-rf file)
                 (install-archive! url dest force?)))

           ; Throw by default
           (throw e)))

       (finally
         ; Clean up tmpdir
         (exec :rm :-rf tmpdir))))
   dest))

(defn install-tarball!
  ([node url dest]
   (install-tarball! node url dest false))
  ([node url dest force?]
   (warn "DEPRECATED: jepsen.control.util/install-tarball! is now named jepsen.control.util/install-archive!, and the `node` argument is no longer required.")
   (install-archive! url dest force?)))

(defn ensure-user!
  "Make sure a user exists."
  [username]
  (try (su (exec :adduser :--disabled-password :--gecos (lit "''") username))
       (catch RuntimeException e
         (when-not (re-find #"already exists" (.getMessage e))
           (throw e))))
  username)

(defn grepkill!
  "Kills processes by grepping for the given string."
  ([pattern]
   (grepkill! 9 pattern))
  ([signal pattern]
   (try
     (exec :ps :aux
           | :grep pattern
           | :grep :-v "grep"
           | :awk "{print $2}"
           | :xargs :kill (str "-" signal))
     ; Occasionally returns nonzero exit status and empty strings for reasons I
     ; don't understand but think are fine?
     (catch RuntimeException e
       (when-not (re-find #"^\s*$" (.getMessage e))
         (throw e))))))

(defn start-daemon!
  "Starts a daemon process, logging stdout and stderr to the given file.
  Invokes `bin` with `args`. Options are:

  :background?
  :chdir
  :logfile
  :make-pidfile?
  :match-executable?
  :match-process-name?
  :pidfile
  :process-name"
  [opts bin & args]
  (info "starting" (.getName (file bin)))
  (apply exec :start-stop-daemon :--start
         (when (:background? opts true) [:--background :--no-close])
         (when (:make-pidfile? opts true) :--make-pidfile)
         (when (:match-executable? opts true) [:--exec bin])
         (when (:match-process-name? opts false)
           [:--name (:process-name opts (.getName (file bin)))])
         :--pidfile  (:pidfile opts)
         :--chdir    (:chdir opts)
         :--oknodo
         :--startas  bin
         :--
         (concat args [:>> (:logfile opts) (lit "2>&1")])))

(defn stop-daemon!
  "Kills a daemon process by pidfile, or, if given a command name, kills all
  processes with that command name, and cleans up pidfile."
  ([pidfile]
   (info "Stopping" pidfile)
   (when (exists? pidfile)
     (let [pid (Long/parseLong (exec :cat pidfile))]
       (meh (exec :kill :-9 pid))
       (meh (exec :rm :-rf pidfile)))))

  ([cmd pidfile]
   (info "Stopping" cmd)
   (meh (exec :killall :-9 :-w cmd))
   (meh (exec :rm :-rf pidfile))))
