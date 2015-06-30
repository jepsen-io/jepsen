(ns jepsen.store
  "Persistent storage for test runs and later analysis."
  (:refer-clojure :exclude [load])
  (:require [clojure.data.fressian :as fress]
            [clojure.pprint :refer [pprint]]
            [clojure.java.io :as io]
            [clj-time.core :as time]
            [clj-time.local :as time.local]
            [clj-time.coerce :as time.coerce]
            [clj-time.format :as time.format]
            [multiset.core :as multiset])
  (:import (java.io File)
           (org.fressian.handlers WriteHandler ReadHandler)
           (multiset.core MultiSet)))

(def dir "store")

(def write-handlers
  (-> {org.joda.time.DateTime
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

       multiset.core.MultiSet
       {"multiset" (reify WriteHandler
                     (write [_ w set]
                       (.writeTag     w "multiset" 1)
                       (.writeObject  w (multiset/multiplicities set))))}}

      (merge fress/clojure-write-handlers)
      fress/associative-lookup
      fress/inheritance-lookup))

(def read-handlers
  (-> {"date-time" (reify ReadHandler
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

       "multiset" (reify ReadHandler
                    (read [_ rdr tag component-count]
                      (multiset/multiplicities->multiset
                        (.readObject rdr))))}

      (merge fress/clojure-read-handlers)
      fress/associative-lookup))

(defn ^File path
  "Under what path should we write a test? Takes either a test or a name and
  time."
  ([test]
   (assert (:name test))
   (assert (:start-time test))
   (path (:name test) (time.local/format-local-time (:start-time test)
                                                    :basic-date-time)))
  ([test-name test-time]
   (io/file dir
            (name test-name)
            (str (name test-time) ".fressian"))))

(defn save!
  "Writes a test to disk. Returns test."
  [test]
  (let [test (dissoc test :db :os :net :client :checker :nemesis :generator :model)
        path (path test)]
    (io/make-parents path)
    (with-open [file   (io/output-stream path)
                out    (fress/create-writer file :handlers write-handlers)]
      (fress/write-object out test)))
    test)

(defn load
  "Loads a specific test by name and time."
  [test-name test-time]
  (with-open [file (io/input-stream (path test-name test-time))
              in   (fress/create-reader file :handlers read-handlers)]
    (fress/read-object in)))

(declare tests)
(defn tests
  "If given a test name, returns a map of test runs to deref-able tests. With
  no test name, returns a map of test names to maps of runs to deref-able
  tests."
  ([]
   (->> (io/file dir)
        (.listFiles)
        (keep #(and (.isDirectory %)
                    (let [name (.getName %)]
                      (and (not= "." name)
                           (not= ".." name)
                           name))))
        (map (juxt identity tests))
        (into {})))
  ([test-name]
   (assert test-name)
   (->> test-name
        name
        (io/file dir)
        file-seq
        (keep #(second (re-matches #"(.+)\.fressian$" (.getName %))))
        (map (fn [f]
               [f (delay (load test-name f))]))
        (into {}))))

(defn delete!
  "Deletes all tests under a given name, or, if given a date as well, a
  specific test."
  ([test-name]
   (dorun (map delete! (repeat test-name) (keys (tests test-name)))))
  ([test-name test-time]
   (.delete (path test-name test-time))))
