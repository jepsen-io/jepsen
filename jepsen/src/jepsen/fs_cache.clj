(ns jepsen.fs-cache
  "Some systems Jepsen tests are expensive or time-consuming to set up. They
  might involve lengthy compilation processes, large packages which take a long
  time to download, or allocate large files on initial startup.

  Other systems require state which persists from run to run--for instance,
  there might be an expensive initial cluster join process, and you might want
  to perform that process once, save the cluster's state to disk before
  testing, and for subsequent tests redeploy that state to nodes to skip the
  cluster join.

  This namespace provides a persistent cache, stored on the control node's
  filesystem, which is suitable for strings, data, or files. It also provides a
  basic locking mechanism.

  Cached values are referred to by logical *paths*: a vector of strings,
  keywords, numbers, booleans, etc; see the Encode protocol for details. For
  instance, a cache path could be any of

    [:hi]
    [1 true]
    [:foodb :license :key]
    [:foodb \"1523a6b\"]

  Cached paths can be stored and retrieved in several ways. Each storage type
  has a pair of functions: (cache/save-string! path \"foo\") writes \"foo\" to
  `path`, and (cache/load-string path) returns the stored value as a string.

  - As strings (save-string!, load-string)
  - As EDN data (save-edn!, load-edn)
  - As raw File objects (save-file!, load-file)
  - As remote files (save-remote!, deploy-remote!)

  In general, writers are `(save-format! from-source to-path)`, and return
  `from-source`, allowing them to be used transparently for side effects.
  Readers are `(load-format path)`, and return `nil` if the value is uncached.
  You can use `(cached? path)` to distinguish between a missing cache value and
  a present `nil`.

  Writes to cache are atomic: a temporary file will be written to first, then
  renamed into its final cache location.

  You can acquire locks on any cache path, whether it exists or not, using
  `(locking path ...)`."
  (:refer-clojure :exclude [load-file load-string locking])
  (:require [clojure [edn :as edn]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [fipp.edn :refer [pprint]]
            [jepsen [control :as c]
                    [util :as util]])
  (:import (java.io File)
           (java.nio.file AtomicMoveNotSupportedException
                          StandardCopyOption
                          Files
                          Path)
           (java.nio.file.attribute FileAttribute)))

;; Where do we store files, and how do we encode paths?

(def ^File dir
  "Top-level cache directory."
  (io/file "/tmp/jepsen/cache"))

(def dir-prefix
  "What string do we prefix to directories in cache path filenames, to
  distinguish /foo from /foo/bar?"
  "d")

(def file-prefix
  "What string do we prefix to files in cache path filenames, to distinguish
  /foo from /foo/bar?"
  "f")

(defn escape
  "Escapes slashes in filenames."
  [string]
  (str/replace string #"(\\|/)" "\\\\$1"))

(defprotocol Encode
  (encode-path-component
    [component]
    "Encodes datatypes to strings which can be used in filenames. Use `escape`
    to escape slashes."))

(extend-protocol Encode
  String
  (encode-path-component [s] (str "s_" (escape s)))

  Boolean
  (encode-path-component [b] (str "b_" b))

  clojure.lang.Keyword
  (encode-path-component [k] (str "k_" (escape (name k))))

  Long
  (encode-path-component [x] (str "l_" x))

  clojure.lang.BigInt
  (encode-path-component [x] (str "n_" x))

  java.math.BigDecimal
  (encode-path-component [x] (str "m_" x)))

(defn fs-path
  "Takes a cache path, and returns a sequence of filenames for that path."
  [path]
  (when-not (sequential? path)
    (throw (IllegalArgumentException. "Cache path must be a sequential collection.")))
  (when-not (seq path)
    (throw (IllegalArgumentException. "Cache path must not be empty.")))
  (let [last-i (dec (count path))]
    (loop [i    0
           path (transient (vec path))]
      (let [component (-> path
                          (nth i)
                          encode-path-component
                          (->> (str (if (= last-i i)
                                      file-prefix
                                      dir-prefix))))
            path (assoc! path i component)]
        (if (= last-i i)
          (persistent! path)
          (recur (inc i) path))))))

;; Turning paths into files, saving files atomically

(defn ^File file
  "The local File backing a given path, whether or not it exists."
  [path]
  (apply io/file dir (fs-path path)))

(defn ^File file!
  "Like file, but ensures parents exist."
  [path]
  (let [f (file path)]
    (io/make-parents f)
    f))

(defn atomic-move!
  "Attempts to move a file atomically, even across filesystems."
  [^File f1 ^File f2]
  (let [p1 ^Path (.toPath f1)
        p2 ^Path (.toPath f2)]
    (try (Files/move p1 p2 (into-array [StandardCopyOption/ATOMIC_MOVE]))
         (catch AtomicMoveNotSupportedException _
           ; Copy to the same directory, then rename
           (let [tmp ^Path (.resolveSibling p2 (str (.getFileName p2)
                                                    ".tmp."
                                                    ; Hopefully good enough
                                                    (System/nanoTime)))]
             (try
               (Files/copy p1 tmp ^"[Ljava.nio.file.StandardCopyOption;"
                           (into-array StandardCopyOption
                                       [StandardCopyOption/COPY_ATTRIBUTES]))
               (Files/move tmp p2
                           (into-array StandardCopyOption
                                       [StandardCopyOption/ATOMIC_MOVE]))
               (Files/deleteIfExists p1)
               (finally
                 (Files/deleteIfExists tmp))))))))

(defmacro write-atomic!
  "Writes a file atomically. Takes a binding form and a body, like so

    (write-atomic [tmp-file (io/file \"final.txt\")]
      (write! tmp-file))

  Creates a temporary file, and binds it to `tmp-file`. Evals body, presumably
  modifying tmp-file in some way. If body terminates normally, renames tmp-file
  to final-file. Ensures temp file is cleaned up."
  [[tmp-sym final] & body]
  `(let [final#   ^File ~final
         ~tmp-sym (-> (.getParent (.toPath final#))
                      (Files/createTempFile (str "." (.getName final#) ".")
                                            ".tmp"
                                            (make-array FileAttribute 0))
                      .toFile)]
     (try ~@body
          (atomic-move! ~tmp-sym final#)
          (finally
            (.delete ~tmp-sym)))))

;; General-purpose API functions

(defn cached?
  "Do we have the given path cached?"
  [path]
  (.isFile (file path)))

(defn clear!
  "Clears the entire cache, or a specified path."
  ([]
   (->> dir
        io/file
        file-seq
        (mapv (memfn ^File delete))))
  ([path]
   (.delete (file path))))

;; Writers and Readers

(defn save-file!
  "Caches a File object to the given path. Returns file."
  [file path]
  (write-atomic! [f (file! path)]
    (io/copy file f))
  file)

(defn ^File load-file
  "The local File backing a given path. Returns nil if no file exists."
  [path]
  (let [f (file path)]
    (when (.isFile f)
      f)))

(defn ^String save-string!
  "Caches the given string to a cache path. Returns string."
  [string path]
  (write-atomic! [f (file! path)]
    (spit f string))
  string)

(defn load-string
  "Returns the cached value for a given path as a string, or nil if uncached."
  [path]
  (when-let [f (load-file path)]
    (slurp f)))

(defn save-edn!
  "Writes the given data structure to an EDN file. Returns data."
  [data path]
  (write-atomic! [f (file! path)]
                 (with-open [w (io/writer f)]
                   (binding [*out* w]
                     (pprint data))))
  data)

(defn load-edn
  "Reads the given cache path as an EDN structure, returning data. Returns nil
  if file does not exist."
  [path]
  (when-let [f (load-file path)]
    (edn/read-string (slurp f))))

(defn save-remote!
  "Caches a remote path (a string) to a cache path by SCPing it down. Returns
  remote-path."
  [remote-path cache-path]
  (write-atomic! [f (file! cache-path)]
    (c/download remote-path (.getCanonicalPath f)))
  remote-path)

(defn deploy-remote!
  "Deploys a cached path to the given remote path (a string). Deletes remote
  path first. Creates parents if necessary."
  [cache-path remote-path]
  (when-not (cached? cache-path)
    (throw (IllegalStateException. (str "Path " (pr-str cache-path) " is not cached and cannot be deployed."))))
  (when-not (re-find #"/\w+/.+" remote-path)
    (throw (IllegalArgumentException.
             (str "You asked to deploy to a remote path " (pr-str remote-path)
                  " which looks relative or suspiciously short--this might be dangerous!"))))

  (c/exec :rm :-rf remote-path)
  (let [parent (.getParent (io/file remote-path))]
    (c/exec :mkdir :-p parent)
    (c/upload (.getCanonicalPath (file cache-path)) remote-path)))

;; Locks

(defonce locks
  (util/named-locks))

(defmacro locking
  "Acquires a lock for a particular cache path, and evaluates body. Helpful for
  reducing concurrent evaluation of expensive cache misses."
  [path & body]
  `(util/with-named-lock locks ~path
     ~@body))
