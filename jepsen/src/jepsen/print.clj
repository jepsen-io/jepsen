(ns jepsen.print
  "Handles printing and logging things as strings or to the console."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [fipp.edn :as fipp.edn :refer [pretty-coll]]
            [fipp.ednize :refer [edn record->tagged]]
            [fipp.visit :refer [visit visit*]]
            [fipp.engine :refer (pprint-document)]
            [jepsen [history :as h]]
            [jepsen.history.fold :refer [loopf]])
  (:import (jepsen.history Op)
           (java.io File
                    RandomAccessFile)))


; Directly from fipp.edn; this is the most compact way I can find to override a
; single datatype's printing.
(defrecord JepsenPrinter [symbols print-meta print-length print-level]
  fipp.visit/IVisitor

  (visit-unknown [this x]
    (visit this (edn x)))

  (visit-nil [this]
    [:text "nil"])

  (visit-boolean [this x]
    [:text (str x)])

  (visit-string [this x]
    [:text (binding [*print-readably* true]
             (pr-str x))])

  (visit-character [this x]
    [:text (binding [*print-readably* true]
             (pr-str x))])

  (visit-symbol [this x]
    [:text (str x)])

  (visit-keyword [this x]
    [:text (str x)])

  (visit-number [this x]
    (binding [*print-dup* false]
      [:text (pr-str x)]))

  (visit-seq [this x]
    (if-let [pretty (symbols (first x))]
      (pretty this x)
      (pretty-coll this "(" x :line ")" visit)))

  (visit-vector [this x]
    (pretty-coll this "[" x :line "]" visit))

  (visit-map [this x]
    (pretty-coll this "{" x [:span "," :line] "}"
      (fn [printer [k v]]
        [:span (visit printer k) " " (visit printer v)])))

  (visit-set [this x]
    (pretty-coll this "#{" x :line "}" visit))

  (visit-tagged [this {:keys [tag form]}]
    [:group "#" (str tag)
            (when (or (and print-meta (meta form))
                      (not (coll? form)))
              " ")
            (visit this form)])

  (visit-meta [this m x]
    (if print-meta
      [:align [:span "^" (visit this m)] :line (visit* this x)]
      (visit* this x)))

  (visit-var [this x]
    [:text (str x)])

  (visit-pattern [this x]
    [:text (pr-str x)])

  (visit-record [this x]
    (if (instance? Op x)
      (fipp.visit/visit-map this x)
      (visit this (record->tagged x)))))

(defn pretty
  ([x] (pretty x {}))
  ([x options]
   (let [defaults {:symbols {}
                   :print-length *print-length*
                   :print-level *print-level*
                   :print-meta *print-meta*}
         printer (map->JepsenPrinter (merge defaults options))]
     (binding [*print-meta* false]
       (visit printer x)))))

(defn pprint
  "Like Fipp's pprint, but with a few Jepsen-specific changes to make output
  more readable."
  ([x] (pprint x {}))
  ([x options]
   (-> (pretty x options)
       (pprint-document options))))

(defn pprint-str
  "Pretty-prints to a string."
  [x]
  (with-out-str (pprint x {:width 78})))

(defn spy
  "Useful for logging in threading macros: logs x (pretty-printed), and returns
  x"
  [x]
  (info (pprint-str x))
  x)

;; Printing histories and logging to the console

(def buf-size 1048576)

(defn concat-files!
  "Appends contents of all fs, writing to out. Returns fs."
  [out fs]
  (with-open [oc (.getChannel (RandomAccessFile. (io/file out) "rw"))]
    (doseq [f fs]
      (with-open [fc (.getChannel (RandomAccessFile. (io/file f) "r"))]
        (let [size (.size fc)]
          (loop [position 0]
            (when (< position size)
              (recur (+ position (.transferTo fc
                                              position
                                              (min (- size position)
                                                   buf-size)
                                              oc)))))))))
  fs)

(defn op->str
  "Format an operation as a string."
  [op]
  (str (:process op)         \tab
       (:type op)            \tab
       (pr-str (:f op))      \tab
       (pr-str (:value op))
       (when-let [err (:error op)]
         (str \tab err))))

(defn prn-op
  "Prints an operation to the console."
  [op]
  (pr (:process op)) (print \tab)
  (pr (:type op))    (print \tab)
  (pr (:f op))       (print \tab)
  (pr (:value op))
  (when-let [err (:error op)]
    (print \tab) (print err))
  (print \newline))

(defn print-history
  "Prints a history to the console."
  ([history]
    (print-history prn-op history))
  ([printer history]
   (doseq [op history]
     (printer op))))

(defn write-history!
  "Writes a history to a file."
  ([f history]
   (write-history! f prn-op history))
  ([f printer history]
   (with-open [w (io/writer f)]
     (binding [*out* w]
       (print-history printer history)))))

(defn pwrite-history!
  "Writes history, taking advantage of more cores."
  ([f history]
    (pwrite-history! f prn-op history))
  ([f printer history]
   (h/fold history
           (loopf {:name [:pwrite-history (str printer)]}
                  ; Reduce
                  ([file   (File/createTempFile "jepsen-history" ".part")
                    writer (io/writer file)]
                   [op]
                   (do (binding [*out*              writer
                                 *flush-on-newline* false]
                         (printer op))
                       (recur file writer))
                   (do (.flush ^java.io.Writer writer)
                       (.close ^java.io.Writer writer)
                       file))
                  ; Combine
                  ([files []]
                   [file]
                   (recur (conj files file))
                   (try (concat-files! f files)
                        f
                        (finally
                          (doseq [^File f files] (.delete f)))))))))

(defn log-op
  "Logs an operation and returns it."
  [op]
  (info (op->str op))
  op)


(def logger (agent nil))

(defn log-print
  [_ & things]
  (apply println things))

(defn log
  [& things]
  (apply send-off logger log-print things))

(defn test->str
  "Pretty-prints a test to a string. This binds *print-length* to avoid printing
  infinite sequences for generators."
  [test]
  ; What we're doing here is basically recreating normal map pretty-printing at
  ; the top level, but overriding generators so that they only print 8 or so
  ; elements.
  (with-out-str
    (fipp.engine/pprint-document
      [:group "{"
       [:nest 1
        (->> test
             (map (fn [[k v]]
                    [:group
                     (pretty k)
                     :line
                     (if (= k :generator)
                       (binding [*print-length* 8]
                         (pretty v))
                       (pretty v))]))
             (interpose [:line ", " ""]))]
        "}"]
      {:width 80})))

; JDK logging

(defn all-jdk-loggers []
  (let [manager (java.util.logging.LogManager/getLogManager)]
    (->> manager
         .getLoggerNames
         java.util.Collections/list
         (map #(.getLogger manager %)))))

(defmacro mute-jdk [& body]
  `(let [loggers# (all-jdk-loggers)
         levels#  (map #(.getLevel %) loggers#)]
     (try
       (doseq [l# loggers#]
         (.setLevel l# java.util.logging.Level/OFF))
       ~@body
       (finally
         (dorun (map (fn [logger# level#] (.setLevel logger# level#))
                     loggers#
                     levels#))))))

(defmacro mute [& body]
  `(mute-jdk
     ~@body))

