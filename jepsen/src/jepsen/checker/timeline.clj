(ns jepsen.checker.timeline
  "Renders an HTML timeline of a history."
  (:require [clojure.core.reducers :as r]
            [clojure.string :as str]
            [clj-time.coerce :as t-coerce]
            [hiccup.core :as h]
            [knossos.history :as history]
            [jepsen.util :as util :refer [name+ pprint-str]]
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
       ".op         { position: absolute; padding: 2px; border-radius: 2px; box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24); transition: all 0.3s cubic-bezier(.25,.8,.25,1); }\n"
       ".op.invoke  { background: #eeeeee; }\n"
       ".op.ok      { background: #6DB6FE; }\n"
       ".op.info    { background: #FFAA26; }\n"
       ".op.fail    { background: #FEB5DA; }\n"
       ".op:target  { box-shadow: 0 14px 28px rgba(0,0,0,0.25), 0 10px 10px rgba(0,0,0,0.22); }\n"))

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

(defn nemesis? [op] (= :nemesis (:process op)))

(defn render-op    [op] (str "Op:\n" (pprint-str op)))
(defn render-msg   [op] (str "Msg: " (pr-str (:value op))))
(defn render-error [op] (str "Err: " (pr-str (:error op))))

(defn render-duration [start stop]
  (let [stop (:time stop)
        start (:time start)
        dur (->> start
                 (- stop)
                 util/nanos->ms
                 long)]
    (str "Dur: " dur " ms")))

(defn render-wall-time [test op]
  (let [start (-> test :start-time t-coerce/to-long)
        op    (-> op :time util/nanos->ms long)
        w     (t-coerce/from-long (+ start op))]
    (str "Wall-clock Time: " w)))

(defn title [test op start stop]
  (str (when (nemesis? op) (render-msg start))
       (when stop          (render-duration start stop))
       "\n"
       (render-error op)
       "\n"
       (render-wall-time test op)
       "\n"
       "\n"
       (render-op op)))

(defn body
  [op start stop]
  (let [same-pair-values? (= (:value start) (:value stop))]
    (str (:process op)
         " "
         (name+ (:f op))
         " "
         (when-not (nemesis? op) (:value start))
         (when-not same-pair-values? (str "<br />" (:value stop))))))

(defn pair->div
  "Turns a pair of start/stop operations into a div."
  [history test process-index [start stop]]
  (let [p (:process start)
        op (or stop start)
        s {:width  col-width
           :left   (* gutter-width (get process-index p))
           :top    (* height (:sub-index start))}]
    [:a {:href (str "#i" (:index op))}
     [:div {:class (str "op " (name (:type op)))
            :id (str "i" (:index op))
            :style (style (cond (= :info (:type stop))
                                (assoc s :height (* height
                                                    (- (inc (count history))
                                                       (:sub-index start))))

                                stop
                                (assoc s :height (* height
                                                    (- (:sub-index stop)
                                                       (:sub-index start))))

                                true
                                (assoc s :height height)))
            :title (title test op start stop)}
      (body op start stop)]]))

(defn linkify-time
  "Remove - and : chars from a time string"
  [t]
  (clojure.string/replace t #"-|:" ""))

(defn breadcrumbs
  "Renders a series of back links increasing in depth"
  [test history-key]
  (let [files-name (str "/files/" (:name test))
        start-time (linkify-time (str (:start-time test)))
        indep      "independent"
        key        (str history-key)]
    [:div
     [:a {:href "/"} "jepsen"] " / "
     [:a {:href files-name} (str (:name test))] " / "
     [:a {:href (str files-name "/" start-time)} start-time] " / "
     [:a {:href (str files-name "/" start-time "/" )} indep] " / "
     [:a {:href (str files-name "/" start-time "/" indep "/" key)} key]]))

(defn process-index
  "Maps processes to columns"
  [history]
  (->> history
       history/processes
       history/sort-processes
       (reduce (fn [m p] (assoc m p (count m)))
               {})))

(defn sub-index
  "Attaches a :sub-index key to each element of this timeline's subhistory,
  identifying its relative position."
  [history]
  (->> history
       (mapv (fn [i op] (assoc op :sub-index i)) (range))
       vec))

(defn html
  []
  (reify checker/Checker
    (check [this test history opts]
      (->> (h/html [:html
                    [:head
                     [:style stylesheet]]
                    [:body
                     (breadcrumbs test (:history-key opts))
                     [:h1 (str (:name test) " key " (:history-key opts))]
                     [:div {:class "ops"}
                      (->> history
                           history/complete
                           sub-index
                           pairs
                           (map (partial pair->div
                                         history
                                         test
                                         (process-index history))))]]])
           (spit (store/path! test (:subdirectory opts) "timeline.html")))
      {:valid? true})))
