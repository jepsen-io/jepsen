(ns jepsen.store.fressian
  "Supports serialization of various Jepsen datatypes via Fressian."
  (:require [clojure.data.fressian :as fress]
            [clojure.java.io :as io]
            [clojure [walk :as walk]]
            [clojure.tools.logging :refer :all]
            [clj-time.local :as time.local]
            [clj-time.format :as time.format]
            [fipp.edn :refer [pprint]]
            [multiset.core :as multiset]
            [jepsen.util :as util])
  (:import (java.util AbstractList)
           (java.time Instant)
           (org.fressian.handlers WriteHandler ReadHandler)
           (multiset.core MultiSet)))

(def write-handlers*
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
                                  (.writeTag w "persistent-hash-set" 1)
                                  (.writeObject w (seq set))))}

       clojure.lang.PersistentTreeSet
       {"persistent-sorted-set" (reify WriteHandler
                                  (write [_ w set]
                                    (.writeTag w "persistent-sorted-set" 1)
                                    (.writeObject w (seq set))))}

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
                       (.writeObject  w (multiset/multiplicities set))))}

      java.time.Instant
      {"instant" (reify WriteHandler
                   (write [_ w instant]
                     (.writeTag w "instant" 1)
                     (.writeObject w (.toString instant))))}}
      (merge fress/clojure-write-handlers)))

(def write-handlers
  (-> write-handlers*
      fress/associative-lookup
      fress/inheritance-lookup))

(def read-handlers*
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
                                 (assert (= 1 component-count))
                                 (into #{} (.readObject rdr))))

       "persistent-sorted-set" (reify ReadHandler
                                 (read [_ rdr tag component-count]
                                   (assert (= 1 component-count))
                                   (into (sorted-set) (.readObject rdr))))

       "map-entry" (reify ReadHandler
                     (read [_ rdr tag component-count]
                       (clojure.lang.MapEntry. (.readObject rdr)
                                               (.readObject rdr))))

       "multiset" (reify ReadHandler
                    (read [_ rdr tag component-count]
                      (multiset/multiplicities->multiset
                        (.readObject rdr))))

       "instant" (reify ReadHandler
                   (read [_ rdr tag component-count]
                     (Instant/parse (.readObject rdr))))}

      (merge fress/clojure-read-handlers)))

(def read-handlers
  (fress/associative-lookup read-handlers*))

(defn postprocess-fressian
  "Fressian likes to give us ArrayLists, which are kind of a PITA when you're
  used to working with vectors. We map those back to vectors again."
  [obj]
  (walk/prewalk (fn transform [x]
                  ; (prn :x (class x) (instance? AbstractList x))
                  (cond (instance? clojure.lang.Atom x)
                        (atom (postprocess-fressian @x))

                        (instance? AbstractList x)
                        (vec x)

                        :else x))
                obj))

(defn reader
  "Creates a Fressian reader given an InputStream. Options:

    :handlers   Read handlers"
  ([input-stream]
   (reader input-stream {}))
  ([input-stream opts]
   (fress/create-reader input-stream
                        :handlers (:handlers opts read-handlers))))

(defn writer
  "Creates a Fressian writer given an OutputStream. Options:

    :handlers   Write handlers"
  ([output-stream]
   (writer output-stream {}))
  ([output-stream opts]
   (fress/create-writer output-stream
                        :handlers (:handlers opts write-handlers))))
