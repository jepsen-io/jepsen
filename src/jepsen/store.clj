(ns jepsen.store
  "Persistent storage for test runs and later analysis."
  (:refer-clojure :exclude [load])
  (:require [clojure.data.fressian :as fress]
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
  (let [test (dissoc test :db :os :client :checker :nemesis :generator :model)
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

(defn tests
  "Returns a map of filenames to deref-able tests."
  [test-name]
  (assert test-name)
  (->> test-name
       name
       (io/file dir)
       file-seq
       (keep #(second (re-matches #"(.+)\.fressian$" (.getName %))))
       (map (fn [f]
              [f (delay (load test-name f))]))
       (into {})))

(defn delete!
  "Deletes all tests under a given name, or, if given a date as well, a
  specific test."
  ([test-name]
   (dorun (map delete! (repeat test-name) (keys (tests test-name)))))
  ([test-name test-time]
   (.delete (path test-name test-time))))
