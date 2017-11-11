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

(defn title [op start stop]
  (str (when (and (= :nemesis (:process op)) (:value start)) (str (:value start) "\n"))
       (when stop (str (long (util/nanos->ms
                              (- (:time stop) (:time start))))
                       " ms\n"))
       (pr-str (:error op))))

(defn body
  [op start stop]
  (str (:process op) " " (name+ (:f op)) " " (when (not= :nemesis (:process op)) (:value start))
       (when (not= (:value start) (:value stop))
         (str "<br />" (:value stop)))))

(defn pair->div
  "Turns a pair of start/stop operations into a div."
  [history process-index [start stop]]
  (let [p (:process start)
        op (or stop start)
        s {:width  col-width
           :left   (* gutter-width (get process-index p))
           :top    (* height (:t-index start))}]
    [:a {:href (str "#i" (:index op))}
     [:div {:class (str "op " (name (:type op)))
            :id (str "i" (:index op))
            :style (style (cond (= :info (:type stop))
                                (assoc s :height (* height
                                                    (- (inc (count history))
                                                       (:t-index start))))

                                stop
                                (assoc s :height (* height
                                                    (- (:t-index stop)
                                                       (:t-index start))))

                                true
                                (assoc s :height height)))
            :title (title op start stop)}
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

(defn t-index
  "Attaches a :t-index key to each element of the history, identifying its
  position in the timeline."
  [history]
  (->> history
       (mapv (fn [i op] (assoc op :t-index i)) (range))
       vec))

(defn html
  []
  (reify checker/Checker
    (check [this test model history opts]
      (->> (h/html [:html
                    [:head
                     [:style stylesheet]]
                    [:body
                     (breadcrumbs test (:history-key opts))
                     [:h1 (str (:name test) " key " (:history-key opts))]
                     [:div {:class "ops"}
                      (->> history
                           history/complete
                           t-index
                           pairs
                           (map (partial pair->div
                                         history
                                         (process-index history))))]]])
           (spit (store/path! test (:subdirectory opts) "timeline.html")))
      {:valid? true})))
