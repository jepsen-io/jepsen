(ns jepsen.checker.timeline
  "Renders an HTML timeline of a history."
  (:require [clojure.core.reducers :as r]
            [clojure.string :as str]
            [hiccup.core :as h]
            [jepsen.util :as util]
            [jepsen.checker :as checker]))

(defn processes
  "What processes are in a history?"
  [history]
  (->> history
       (r/map :process)
       (into #{})))

(defn pairs
  "Yields a lazy sequence of [info] | [invoke, ok|fail] pairs from a history]"
  ([history]
   (pairs {} history))
  ([invocations [op & ops]]
   (lazy-seq
     (when op
       (case (:type op)
         :info        (cons [op] (pairs invocations ops))
         :invoke      (do (assert (not (contains? invocations (:process op))))
                          (pairs (assoc invocations (:process op) op) ops))
         (:ok :fail)  (do (assert (contains? invocations (:process op)))
                          (cons [(get invocations (:process op)) op]
                                (pairs (dissoc invocations (:process op))
                                       ops))))))))

(defn style
  "Generate a CSS style fragment from a map."
  [m]
  (->> m
       (map (fn [kv] (str (name (key kv)) ":" (val kv))))
       (str/join ";")))

(def timescale "Nanoseconds per pixel" 1e6)
(def col-width "pixels" 100)

(def stylesheet
  (str ".ops        { position: absolute; }\n"
       ".op         { position: absolute; }\n"
       ".op.invoke  { background: #C1DEFF; }\n"
       ".op.ok      { background: #B7FFB7; }\n"
       ".op.fail    { background: #FFD4D5; }\n"
       ".op.info    { background: #FEFFC1; }\n"))

(defn pair->div
  "Turns a pair of start/stop operations into a div."
  [process-index [start stop]]
  (let [p (:process start)
        op (or stop start)]
    (when (:time start)
      [:div {:class (str "op " (name (:type op)))
             :style (style {:width  col-width
                            :left   (* col-width (get process-index p))
                            :height (if stop
                                      (/ (- (:time stop) (:time start))
                                         timescale)
                                      16)
                            :top    (/ (:time start) timescale)})
             :title (when stop (str (long (util/nanos->ms
                                            (- (:time stop) (:time start))))
                                    " ms"))}
       (str (:process op) ": " (:f op) " " (:value op))])))

(defn process-index
  "Maps processes to columns"
  [history]
  (->> (processes history)
       (sort (fn [a b]
               (if (keyword? a)
                 (if (keyword? b)
                   (compare a b)
                   -1)
                 (if (keyword? b)
                   1
                   (compare a b)))))
  (reduce (fn [m p] (assoc m p (count m)))
               {})))

(def html
  (reify checker/Checker
    (check [this test model history]
      (->> (h/html [:html
                    [:head
                     [:style stylesheet]]
                    [:body
                     [:h1 (:name test)]
                     [:p  (str (:start-time test))]
                     [:div {:class "ops"}
                      (->> history
                           pairs
                           (map (partial pair->div
                                         (process-index history))))]]])
           (spit "timeline.html"))
      {:valid? true})))
