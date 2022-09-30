(ns jepsen.control.scp
  "Built-in JDK SSH libraries can be orders of magnitude slower than plain old
  SCP for copying even medium-sized files of a few GB. This provides a faster
  implementation of a Remote which shells out to SCP."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.util :as util]
            [jepsen.control.core :as core]
            [slingshot.slingshot :refer [try+ throw+]]))

(def tmp-dir
  "The remote directory we temporarily store files in while transferring up and
  down."
  "/tmp/jepsen/scp")

(defn exec!
  "A super basic exec implementation for our own purposes. At some point we
  might want to pull some? all? of control/exec all the way down into
  control.remote, and get rid of this."
  [remote ctx cmd-args]
  (->> cmd-args
       (map core/escape)
       (str/join " ")
       (hash-map :cmd)
       (core/wrap-sudo ctx)
       (core/execute! remote ctx)
       core/throw-on-nonzero-exit))

(defmacro with-tmp-dir
  "Evaluates body. If a nonzero exit status occurs, forces the tmp dir to
  exist, and re-evals body. We do this to avoid the overhead of checking for
  existence every time someone wants to upload/download a file."
  [remote ctx & body]
  `(try+ ~@body
        (catch (#{:jepsen.control/nonzero-exit
                  :jepsen.util/nonzero-exit}
                 (:type ~'%)) e#
          (exec! ~remote ~ctx [:mkdir :-p tmp-dir])
          (exec! ~remote ~ctx [:chmod "a+rwx" tmp-dir])
          ~@body)))

(defn tmp-file
  "Returns a randomly generated tmpfile for use during uploads/downloads"
  []
  (str tmp-dir "/" (rand-int Integer/MAX_VALUE)))

(defmacro with-tmp-file
  "Evaluates body with tmp-file-sym bound to the remote path of a temporary
  file. Cleans up file at exit."
  [remote ctx [tmp-file-sym] & body]
  `(let [~tmp-file-sym (tmp-file)
         ; We're going to want to do our tmpfile management as root in case
         ; /tmp/jepsen already exists and we don't own it. Blegh.
         ctx# (assoc ~ctx :sudo "root")]
     (try (with-tmp-dir ~remote ctx# ~@body)
          (finally
            (exec! ~remote ctx# [:rm :-f ~tmp-file-sym])))))

(defn scp!
  "Runs an SCP command by shelling out. Takes a conn-spec (used for port, key,
  etc), a seq of sources, and a single destination, all as strings."
  [conn-spec sources dest]
  (apply util/sh "scp" "-rpC"
         "-P" (str (:port conn-spec))
         (concat (when-let [k (:private-key-path conn-spec)]
                   ["-i" k])
                 (if-not (:strict-host-key-checking conn-spec)
                   ["-o StrictHostKeyChecking=no"])
                 sources
                 [dest]))
  nil)

(defn remote-path
  "Returns the string representation of a remote path using a conn spec; e.g.
  admin@n1:/foo/bar"
  [{:keys [username host]} path]
  (assert host "No node given for remote-path!")
  (str (when username
         (str username "@"))
       host ":" path))

(defrecord Remote [cmd-remote conn-spec]
  core/Remote
  (connect [this conn-spec]
    (-> this
        (assoc :conn-spec conn-spec)
        (update :cmd-remote core/connect conn-spec)))

  (disconnect! [this]
    (update this :cmd-remote core/disconnect!))

  (execute! [this ctx action]
    (core/execute! cmd-remote ctx action))

  (upload! [this ctx srcs dest _]
    (let [sudo (:sudo ctx)]
      (if (or (nil? sudo) (= sudo (:user conn-spec)))
        ; We can upload directly using our connection credentials.
        (scp! conn-spec
              (util/coll srcs)
              (remote-path conn-spec dest))

        ; We need to become a different user for this. Upload each source to a
        ; tmpfile and rename.
        (with-tmp-file cmd-remote ctx [tmp]
          (doseq [src (util/coll srcs)]
            ; Upload to tmpfile
            (core/upload! this {} src tmp nil)
            ; Chown and move to dest, as root
            (exec! cmd-remote {:sudo "root"} [:chown sudo tmp])
            (exec! cmd-remote {:sudo "root"} [:mv tmp dest]))))))

  (download! [this ctx srcs dest _]
    (let [sudo (:sudo ctx)]
      (if (or (nil? sudo) (= sudo (:user conn-spec)))
        ; We can download directly using our conn credentials.
        (scp! conn-spec
              (->> (util/coll srcs)
                   (map (partial remote-path conn-spec)))
              dest)
        ; We need to copy each file to a tmpfile we CAN read before downloading
        ; it.
        (doseq [src (util/coll srcs)]
          (with-tmp-file cmd-remote ctx [tmp]
            ; See if we can read this source as the current user, even if
            ; it's not our own file
            (if (try+ (exec! cmd-remote {} [:head :-n 1 src]) true
                      (catch [:exit 1] _                      false))
              ; We can directly download this file.
              (core/download! this {} src dest nil)
              ; Nope; gotta copy. Try a hardlink?
              (do (try+ (exec! cmd-remote {:sudo "root"} [:ln :-L src tmp])
                        (catch [:exit 1] _
                          ; Fine, maybe a different fs. Try a full copy.
                          (exec! cmd-remote {:sudo "root"}
                                 [:cp src tmp])))
                  ; Make the tmpfile readable to us
                  (exec! cmd-remote {:sudo "root"} [:chown sudo tmp])
                  ; Download it
                  (core/download! this {} tmp dest nil)))))))))

(defn remote
  "Takes a remote which can execute commands, and wraps it in a remote which
  overrides upload & download to use SCP."
  [cmd-remote]
  (Remote. cmd-remote nil))
