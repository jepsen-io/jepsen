(ns jepsen.checker.timeline
  "Renders an HTML timeline of a history."
  (:require [clojure.core.reducers :as r]
            [clojure.string :as str]
            [hiccup.core :as h]
            [knossos.history :as history]
            [jepsen.util :as util :refer [name+]]
            [jepsen.store :as store]
            [jepsen.checker :as checker]))

(defn style
  "Generate a CSS style fragment from a map."
  [m]
  (->> m
       (map (fn [kv] (str (name (key kv)) ":" (val kv))))
       (str/join ";")))

(def timescale    "Nanoseconds per pixel" 1e6)
(def col-width    "pixels" 100)
(def gutter-width "pixels" 106)
(def height       "pixels" 16)

(def stylesheet
  (str ".ops        { position: absolute; }\n"
       ".op         { position: absolute;
                      padding:  2px; }\n"
       ".op.invoke  { background: #eeeeee; }\n"
       ".op.ok      { background: #6DB6FE; }\n"
       ".op.info    { background: #FFAA26; }\n"
       ".op.fail    { background: #FEB5DA; }\n"))

(defn pairs
  "Pairs up ops from each process in a history. Yields a lazy sequence of [info]
  or [invoke, ok|fail|info] pairs."
  ([history]
   (pairs {} history))
  ([invocations [op & ops]]
   (lazy-seq
     (when op
       (case (:type op)
         :info        (if (contains? invocations (:process op))
                        ; Info following invoke
                        (cons [(get invocations (:process op)) op]
                              (pairs (dissoc invocations (:process op)) ops))
                        ; Unmatched info
                        (cons [op] (pairs invocations ops)))
         :invoke      (do (assert (not (contains? invocations (:process op))))
                          (pairs (assoc invocations (:process op) op) ops))
         (:ok :fail)  (do (assert (contains? invocations (:process op)))
                          (cons [(get invocations (:process op)) op]
                                (pairs (dissoc invocations (:process op))
                                       ops))))))))

(defn pair->div
  "Turns a pair of start/stop operations into a div."
  [history process-index [start stop]]
  (let [p (:process start)
        op (or stop start)
        s {:width  col-width
           :left   (* gutter-width (get process-index p))
           :top    (* height (:index start))}]
    [:div {:class (str "op " (name (:type op)))
           :style (style (cond (= :info (:type stop))
                               (assoc s :height (* height
                                                   (- (inc (count history))
                                                      (:index start))))

                               stop
                               (assoc s :height (* height
                                                   (- (:index stop)
                                                      (:index start))))

                               true
                               (assoc s :height height)))
           :title (str (when stop (str (long (util/nanos->ms
                                               (- (:time stop) (:time start))))
                                       " ms\n"))
                       (pr-str (:error op)))}
     (str (:process op) " " (name+ (:f op)) " " (:value start)
          (when (not= (:value start) (:value stop))
            (str "<br />" (:value stop))))]))

(defn process-index
  "Maps processes to columns"
  [history]
  (->> history
       history/processes
       history/sort-processes
       (reduce (fn [m p] (assoc m p (count m)))
               {})))

(defn html
  []
  (reify checker/Checker
    (check [this test model history opts]
      (->> (h/html [:html
                    [:head
                     [:style stylesheet]]
                    [:body
                     [:h1 (:name test)]
                     [:p  (str (:start-time test))]
                     [:div {:class "ops"}
                      (->> history
                           history/complete
                           history/index
                           pairs
                           (map (partial pair->div
                                         history
                                         (process-index history))))]]])
           (spit (store/path! test (:subdirectory opts) "timeline.html")))
      {:valid? true})))
