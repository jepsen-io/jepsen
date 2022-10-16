(ns jepsen.generator.translation-table
  "We burn a lot of time in hashcode and map manipulation for thread names,
  which are mostly integers 0...n, but sometimes non-integer names like
  :nemesis. It's nice to be able to represent thread state internally as purely
  integers. To do this, we compute a one-time translation table which lets us
  map those names to integers and vice-versa."
  (:require [clojure.core.protocols :refer [Datafiable]]
            [clojure [datafy :refer [datafy]]]
            [dom-top.core :refer [loopr]]
            [potemkin :refer [def-map-type definterface+]])
  (:import (io.lacuna.bifurcan ISet
                               IMap
                               Map
                               Set)
           (java.util BitSet)))

(deftype TranslationTable
  [; Number of numeric threads
   ^int     int-thread-count
   ; Array of all threads which *aren't* integers; e.g. :nemesis
   ^objects named-threads
   ; Map of named threads to their indices
   ^IMap    named-thread->index]

  Datafiable
  (datafy [this]
    {:int-thread-count    int-thread-count
     :named-threads       (vec named-threads)
     :named-thread->index (datafy named-thread->index)}))

(defn translation-table
  "Takes a number of integer threads and a collection of named threads, and
  computes a translation table."
  [int-thread-count named-threads]
  (let [named-threads-array (object-array (count named-threads))]
    (loopr [^IMap named-thread->index (.linear Map/EMPTY)
            i                         0]
           [thread named-threads]
           (do (aset named-threads-array i thread)
               (recur (.put named-thread->index thread (int i))
                      (inc i)))
           (TranslationTable. int-thread-count
                              named-threads-array
                              (.forked named-thread->index)))))

(defn all-names
  "A sequence of all names in the translation table, in the exact order of
  thread indices. Index 0's name comes first, then 1, and so on."
  [^TranslationTable translation-table]
  (concat (range (.int-thread-count translation-table))
          (.named-threads translation-table)))

(defn thread-count
  "How many threads in a translation table in all?"
  ^long [^TranslationTable translation-table]
  (let [^objects named-threads (.named-threads translation-table)]
    (+ (.int-thread-count translation-table)
       (alength named-threads))))

(defn name->index
  "Turns a thread name (e.g. 0, 5, or :nemesis) into a primitive int."
  ^long [^TranslationTable translation-table thread-name]
  (if (integer? thread-name)
    thread-name
    (let [^IMap m (.named-thread->index translation-table)
          ; We're not doing bounds checks but we DO want this to blow up
          ; obviously
          i (.get m thread-name Long/MIN_VALUE)]
        (+ (.int-thread-count translation-table)))))

(defn index->name
  "Turns a thread index (an int) into a thread name (e.g. 0, 5, or :nemesis)."
  [^TranslationTable translation-table ^long thread-index]
  (let [itc (.int-thread-count translation-table)]
    (if (< thread-index itc)
      thread-index
      (aget ^objects (.named-threads translation-table) (- thread-index itc)))))

(defn ^ISet indices->names
  "Takes a translation table and a BitSet of thread indices. Constructs a
  Bifurcan ISet out of those threads."
  [translation-table ^BitSet indices]
  (loop [i           0
         ^ISet names (.linear Set/EMPTY)]
        (let [i' (.nextSetBit indices i)]
          (if (= i' -1)
            (.forked names)
            (recur (inc i')
                   (.add names (index->name translation-table i')))))))

(defn ^BitSet names->indices
  "Takes a translation table and a collection of thread names. Constructs a
  BitSet of those thread indices."
  [translation-table names]
  (let [bs (BitSet. (count names))]
    (loopr []
           [name names]
           (do (.set bs (name->index translation-table name))
               (recur)))
    bs))
