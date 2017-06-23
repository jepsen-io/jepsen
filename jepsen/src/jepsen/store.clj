(ns jepsen.store
  "Persistent storage for test runs and later analysis."
  (:refer-clojure :exclude [load])
  (:require [clojure.data.fressian :as fress]
            [clojure.pprint :refer [pprint]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clj-time.core :as time]
            [clj-time.local :as time.local]
            [clj-time.coerce :as time.coerce]
            [clj-time.format :as time.format]
            [unilog.config :as unilog]
            [multiset.core :as multiset]
            [jepsen.util :as util])
  (:import (java.io File)
           (java.nio.file Files
                          FileSystems
                          Path)
           (java.nio.file.attribute FileAttribute
                                    PosixFilePermissions)
           (org.fressian.handlers WriteHandler ReadHandler)
           (multiset.core MultiSet)))

(def base-dir "store")

(def write-handlers
  (-> {clojure.lang.Atom
       {"atom" (reify WriteHandler
                 (write [_ w a]
                   (.writeTag    w "atom" 1)
                   (.writeObject w @a)))}

       org.joda.time.DateTime
       {"date-time" (reify WriteHandler
                      (write [_ w t]
                        (.writeTag    w "date-time" 1)
                        (.writeObject w (time.local/format-local-time
                                          t :basic-date-time))))}

       clojure.lang.PersistentHashSet
       {"persistent-hash-set" (reify WriteHandler
                                (write [_ w set]
                                  (.writeTag w "persistent-hash-set"
                                             (count set))
                                  (doseq [e set]
                                    (.writeObject w e))))}

       clojure.lang.PersistentTreeSet
       {"persistent-sorted-set" (reify WriteHandler
                                  (write [_ w set]
                                    (.writeTag w "persistent-sorted-set"
                                               (count set))
                                    (doseq [e set]
                                      (.writeObject w e))))}

       clojure.lang.MapEntry
       {"map-entry" (reify WriteHandler
                      (write [_ w e]
                        (.writeTag    w "map-entry" 2)
                        (.writeObject w (key e))
                        (.writeObject w (val e))))}

       multiset.core.MultiSet
       {"multiset" (reify WriteHandler
                     (write [_ w set]
                       (.writeTag     w "multiset" 1)
                       (.writeObject  w (multiset/multiplicities set))))}}

      (merge fress/clojure-write-handlers)
      fress/associative-lookup
      fress/inheritance-lookup))

(def read-handlers
  (-> {"atom"      (reify ReadHandler
                     (read [_ rdr tag component-count]
                       (atom (.readObject rdr))))

       "date-time" (reify ReadHandler
                     (read [_ rdr tag component-count]
                       (time.format/parse
                         (:basic-date-time time.local/*local-formatters*)
                         (.readObject rdr))))

       "persistent-hash-set" (reify ReadHandler
                               (read [_ rdr tag component-count]
                                 (let [s (transient #{})]
                                   (dotimes [_ component-count]
                                     (conj! s (.readObject rdr)))
                                   (persistent! s))))

       "persistent-sorted-set" (reify ReadHandler
                                 (read [_ rdr tag component-count]
                                   (loop [i component-count
                                          s (sorted-set)]
                                     (if (pos? i)
                                       (recur (dec i)
                                              (conj s (.readObject rdr)))
                                       s))))

       "map-entry" (reify ReadHandler
                     (read [_ rdr tag component-count]
                       (clojure.lang.MapEntry. (.readObject rdr)
                                               (.readObject rdr))))

       "multiset" (reify ReadHandler
                    (read [_ rdr tag component-count]
                      (multiset/multiplicities->multiset
                        (.readObject rdr))))}

      (merge fress/clojure-read-handlers)
      fress/associative-lookup))

(defn ^File path
  "With one arg, a test, returns the directory for that test's results. Given
  additional arguments, returns a file with that name in the test directory.
  Nested paths are flattened: (path t [:a [:b :c] :d) expands to .../a/b/c/d.
  Nil path components are ignored: (path t :a nil :b) expands to .../a/b.

  Test must have only two keys: :name, and :start-time. :start-time may be a
  string, or a DateTime."
  ([test]
   (assert (:name test))
   (assert (:start-time test))
   (io/file base-dir
            (:name test)
            (let [t (:start-time test)]
              (if (string? t)
                t
                (time.local/format-local-time t :basic-date-time)))))
  ([test & args]
   (->> args
        flatten
        (remove nil?)
        (map str)
        (apply io/file (path test)))))

(defn ^File path!
  "Like path, but ensures the path's containing directories exist."
  [& args]
  (let [path (apply path args)]
    (io/make-parents path)
    path))

(defn ^File fressian-file
  "Gives the path to a fressian file encoding all the results from a test."
  [test]
  (path test "test.fressian"))

(defn ^File fressian-file!
  "Gives the path to a fressian file encoding all the results from a test,
  ensuring its containing directory exists."
  [test]
  (path! test "test.fressian"))

(def default-nonserializable-keys
  "What keys in a test can't be serialized to disk, by default?"
  #{:db :os :net :client :checker :nemesis :generator :model})

(defn nonserializable-keys
  "What keys in a test can't be serialized to disk? The union of default
  nonserializable keys, plus any in :nonserializable-keys."
  [test]
  (into default-nonserializable-keys (:nonserializable-keys test)))

(defn load
  "Loads a specific test by name and time."
  [test-name test-time]
  (with-open [file (io/input-stream (fressian-file {:name       test-name
                                                    :start-time test-time}))]
    (let [in (fress/create-reader file :handlers read-handlers)]
      (fress/read-object in))))

(defn load-results
  "Loads only a results.edn by name and time."
  [test-name test-time]
  (with-open [file (java.io.PushbackReader.
                     (io/reader (path {:name       test-name
                                       :start-time test-time}
                                      "results.edn")))]
    (clojure.edn/read file)))

(defn dir?
  "Is this a directory?"
  [^File f]
  (.isDirectory f))

(defn file-name
  "Maps a File to a string name."
  [^File f]
  (.getName f))

(defn virtual-dir?
  "Is this a . or .. directory entry?"
  [f]
  (let [n (file-name f)]
    (or (= n ".")
        (= n ".."))))

(defn symlink?
  "Is this a symlink?"
  [^File f]
  (Files/isSymbolicLink (.toPath f)))

(defn test-names
  "Returns a seq of all known test names."
  []
  (->> (io/file base-dir)
       (.listFiles)
       (remove virtual-dir?)
       (remove symlink?)
       (filter dir?)
       (map file-name)))

(defn tests
  "If given a test name, returns a map of test runs to deref-able tests. With
  no test name, returns a map of test names to maps of runs to deref-able
  tests."
  ([]
   (->> (test-names)
        (map (juxt identity tests))
        (into {})))
  ([test-name]
   (assert test-name)
   (->> test-name
        name
        (io/file base-dir)
        (.listFiles)
        (remove virtual-dir?)
        (remove symlink?)
        (filter dir?)
        (map file-name)
        (map (fn [f] [f (delay (load test-name f))]))
        (into {}))))

(defn update-symlinks!
  "Creates `latest` symlinks to the given test, if a store directory exists."
  [test]
  (doseq [dest [["latest"] [(:name test) "latest"]]]
    ; did you just tell me to go fuck myself
    (when (.exists (path test))
      (let [src  (.toPath (path test))
            dest (.. FileSystems
                     getDefault
                     (getPath base-dir (into-array dest)))]
        (Files/deleteIfExists dest)
        (Files/createSymbolicLink dest (.relativize (.getParent dest) src)
                                  (make-array FileAttribute 0))))))

(defmacro with-out-file
  "Binds stdout to a file for the duration of body."
  [test filename & body]
  `(let [filename# (path! ~test ~filename)]
     (with-open [w# (io/writer filename#)]
       (try
         (binding [*out* w#] ~@body)
         (finally
           (info "Wrote" (.getCanonicalPath filename#)))))))

(defn write-results!
  "Writes out a results.edn file."
  [test]
  (with-out-file test "results.edn"
    (pprint (:results test))))

(defn write-history!
  "Writes out history.txt and history.edn files."
  [test]
  (util/pwrite-history! (path! test "history.txt") (:history test))
  (util/pwrite-history! (path! test "history.edn") prn (:history test)))

(defn write-fressian!
  "Write the entire test as a .fressian file"
  [test]
  (let [test (apply dissoc test (nonserializable-keys test))]
    (with-open [file   (io/output-stream (fressian-file! test))]
      (let [out (fress/create-writer file :handlers write-handlers)]
        (fress/write-object out test)))))

(defn save-1!
  "Writes a history and fressian file to disk and updates latest symlinks.
  Returns test."
  [test]
  (->> [(future (util/with-thread-name "jepsen history"
                  (write-history! test)))
        (future (util/with-thread-name "jepsen fressian"
                  (write-fressian! test)))]
       (map deref)
       dorun)
  (update-symlinks! test)
  test)

(defn save-2!
  "Phase 2: after computing results, we re-write the fressian file and also
  dump results as edn. Returns test."
  [test]
  (->> [(future (util/with-thread-name "jepsen results" (write-results! test)))
        (future (util/with-thread-name "jepsen fressian"
                  (write-fressian! test)))]
       (map deref)
       dorun)
  (update-symlinks! test)
  test)

(def console-appender
  {:appender :console
   :pattern "%p\t[%t] %c: %m%n"})

(defn start-logging!
  "Starts logging to a file in the test's directory."
  [test]
  (unilog/start-logging!
    {:level   "info"
     :console   false
     :appenders [console-appender
                 {:appender :file
                  :encoder :pattern
                  :pattern "%d{ISO8601}{GMT}\t%p\t[%t] %c: %m%n"
                  :file (.getCanonicalPath (path! test "jepsen.log"))}]}))

(defn stop-logging!
  "Resets logging to console only."
  []
  (unilog/start-logging!
    {:level "info"
     :console   false
     :appenders [console-appender]}))

(defn delete-file-recursively!
  [^File f]
  (let [func (fn [func ^File f]
               (when (.isDirectory f)
                 (doseq [f2 (.listFiles f)]
                   (func func f2)))
               (clojure.java.io/delete-file f))]
    (func func (clojure.java.io/file f))))

(defn delete!
  "Deletes all tests, or all tests under a given name, or, if given a date as
  well, a specific test."
  ([]
   (dorun (map delete! (test-names))))
  ([test-name]
   (dorun (map delete! (repeat test-name) (keys (tests test-name)))))
  ([test-name test-time]
   (delete-file-recursively! (path {:name test-name, :start-time test-time}))))
