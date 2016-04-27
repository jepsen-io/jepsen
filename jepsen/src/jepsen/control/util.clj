(ns jepsen.control.util
  "Utility functions for scripting installations."
  (:require [jepsen.control :refer :all]
            [jepsen.util :refer [meh]]
            [clojure.java.io :refer [file]]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]))

(defn file?
  "Is a file present?"
  [filename]
  (try (exec :stat filename)
       true
       (catch RuntimeException _ false)))

(defn ls
  "A seq of directory entries (not including . and ..). TODO: escaping for
  control chars in filenames (if you do this, WHO ARE YOU???)"
  [dir]
  (->> (str/split (exec :ls :-A dir) #"\n")
       (remove str/blank?)))

(defn ls-full
  "Like ls, but prepends dir to each entry."
  [dir]
  (let [dir (if (re-find #"/$" dir)
              dir
              (str dir "/"))]
    (->> dir
         ls
         (map (partial str dir)))))

(defn wget!
  "Downloads a string URL and returns the filename as a string. Skips if the
  file already exists."
  [url]
  (let [filename (.getName (file url))]
    (when-not (file? filename)
      (exec :wget
            :--tries 20
            :--waitretry 60
            :--retry-connrefused
            :--dns-timeout 60
            :--connect-timeout 60
            :--read-timeout 60
            url))
    filename))

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

  :logfile
  :pidfile
  :chdir"
  [opts bin & args]
  (info "starting" (.getName (file bin)))
  (apply exec :start-stop-daemon :--start
         :--background
         :--make-pidfile
         :--pidfile  (:pidfile opts)
         :--chdir    (:chdir opts)
         :--no-close
         :--oknodo
         :--exec     bin
         :--
         (concat args [:>> (:logfile opts) (lit "2>&1")])))

(defn stop-daemon!
  "Kills a daemon process by command name, and cleans up pidfile."
  [cmd pidfile]
  (info "Stopping" cmd)
  (meh (exec :pkill :-9 cmd))
  (meh (exec :rm :-rf pidfile)))
