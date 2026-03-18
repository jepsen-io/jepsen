(ns jepsen.print
  "Handles printing things as strings or to the console."
  (:require [clojure.string :as str]
            [fipp.edn :as fipp.edn :refer [pretty-coll]]
            [fipp.ednize :refer [edn record->tagged]]
            [fipp.visit :refer [visit visit*]]
            [fipp.engine :refer (pprint-document)])
  (:import (jepsen.history Op)))

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
