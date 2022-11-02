(ns jepsen.checker.clock
  "Helps analyze clock skew over time."
  (:require [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [jepsen [history :as h]
                    [util :as util]
                    [store :as store]]
            [jepsen.checker.perf :as perf]
            [gnuplot.core :as g]))

(defn history->datasets
  "Takes a history and produces a map of nodes to sequences of [t offset]
  pairs, representing the changing clock offsets for that node over time."
  [history]
  (let [final-time (util/nanos->secs (:time (peek history)))]
    (->> history
         (h/filter :clock-offsets)
         (reduce (fn [series op]
                   (let [t (util/nanos->secs (:time op))]
                     (reduce (fn [series [node offset]]
                               (let [s (get series node (transient []))
                                     s (conj! s [t offset])]
                                 (assoc! series node s)))
                             series
                             (:clock-offsets op))))
                 (transient {}))
         persistent!
         (util/map-vals (fn seal [points]
                          (-> points
                              (conj! (assoc (nth points (dec (count points)))
                                            0 final-time))
                              (persistent!)))))))

(defn short-node-names
  "Takes a collection of node names, and maps them to shorter names by removing
  common trailing strings (e.g. common domains)."
  [nodes]
  (->> nodes
       (map #(str/split % #"\."))
       (map reverse)
       util/drop-common-proper-prefix
       (map reverse)
       (map (partial str/join "."))))

(defn plot!
  "Plots clock offsets over time. Looks for any op with a :clock-offset field,
  which contains a (possible incomplete) map of nodes to offsets, in seconds.
  Plots those offsets over time."
  [test history opts]
  (when (seq history)
    ; If the history is empty, don't render anything.
    (let [datasets    (history->datasets history)
          nodes       (util/polysort (keys datasets))
          node-names  (short-node-names nodes)
          output-path (.getCanonicalPath (store/path! test (:subdirectory opts)
                                                      "clock-skew.png"))
          plot {:preamble (concat (perf/preamble output-path)
                                  [[:set :title (str (:name test)
                                                     " clock skew")]
                                   [:set :ylabel "Skew (s)"]])
                :series   (map (fn [node node-name]
                                 {:title node-name
                                  :with  :steps
                                  :data  (get datasets node)})
                               nodes
                               node-names)}]
      (when (perf/has-data? plot)
        (-> plot
            (perf/without-empty-series)
            (perf/with-range)
            (perf/with-nemeses history (:nemeses (:plot test)))
            (perf/plot!)))))
  {:valid? true})
