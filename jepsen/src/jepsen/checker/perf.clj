(ns jepsen.checker.perf
  "Supporting functions for performance analysis."
  (:require [clojure.stacktrace :as trace]
            [fipp.edn :refer [pprint]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [gnuplot.core :as g]
            [jepsen [history :as h]
                    [store :as store]
                    [util :as util]]
            [jepsen.history.fold :as f]
            [multiset.core :as multiset]
            [slingshot.slingshot :refer [try+ throw+]]
            [tesser.core :as t])
  (:import (jepsen.history Op)))

(def default-nemesis-color "#cccccc")
(def nemesis-alpha 0.6)

(defn bucket-scale
  "Given a bucket size dt, and a bucket number (e.g. 0, 1, ...), returns the
  time at the midpoint of that bucket."
  [dt b]
  (-> b long (* dt) (+ (/ dt 2))))

(defn bucket-time
  "Given a bucket size dt and a time t, computes the time at the midpoint of
  the bucket this time falls into."
  [dt t]
  (bucket-scale dt (/ t dt)))

(defn buckets
  "Given a bucket size dt, emits a lazy sequence of times at the midpoints of
  each bucket."
  ([dt]
   (->> (iterate inc 0)
       (map (partial bucket-scale dt))))
  ([dt tmax]
   (take-while (partial >= tmax) (buckets dt))))

(defn bucket-points
  "Takes a time window dt and a sequence of [time, _] points, and emits a
  seq of [time, points-in-window] buckets, ordered by time. Time is at the
  midpoint of the window."
  [dt points]
  (->> points
       (group-by #(->> % first (bucket-time dt)))
       (into (sorted-map))))

(defn quantiles
  "Takes a sequence of quantiles from 0 to 1 and a sequence of values, and
  returns a map of quantiles to values at those quantiles."
  [qs points]
  (let [sorted (sort points)]
    (when-not (empty? sorted)
      (let [n (count sorted)
            extract (fn [q]
                      (let [idx (min (dec n) (long (Math/floor (* n q))))]
                        (nth sorted idx)))]
        (zipmap qs (map extract qs))))))

(defn latencies->quantiles
  "Takes a time window in seconds, a sequence of quantiles from 0 to 1, and a
  sequence of [time, latency] pairs. Groups pairs by their time window and
  emits a emits a map of quantiles to sequences of [time,
  latency-at-that-quantile] pairs, one per time window."
  [dt qs points]
  (assert (number? dt))
  (assert (every? number? qs))
  (assert (every? #(<= 0 % 1) qs))
  (let [buckets (->> points
                     (bucket-points dt)
                     (map (fn [[bucket-time points]]
                            [bucket-time (quantiles qs (map second points))])))]
    ; At this point we have a sequence of
    ; [time, {q1 v1, q2 ; v2, ...}, ...]
    ; pairs, and we want a map of
    ; {q1 -> [[time, v1], [time2, v2], ...], ...}
    (->> qs
         (map (fn [q]
                (map (fn [[t qs]]
                       [t (get qs q)])
                     buckets)))
         (zipmap qs))))

(defn first-time
  "Takes a history and returns the first :time in it, in seconds, as a double."
  [history]
  (when-let [t (->> history
                    (keep :time)
                    first)]
    (double (util/nanos->secs t))))

(defn invokes-by-type
  "Splits up a sequence of invocations into ok, failed, and crashed ops by
  looking at their corresponding completions. Either a tesser fold, or runs on
  a history."
  ([]
   (->> (t/filter h/invoke?)
        (t/fuse {:ok   (t/into [] (t/filter (comp h/ok?   :completion)))
                 :info (t/into [] (t/filter (comp h/info? :completion)))
                 :fail (t/into [] (t/filter (comp h/fail? :completion)))})))
  ([history]
   (h/tesser history (invokes-by-type))))

(defn invokes-by-f
  "Takes a history and returns a map of f -> ops, for all invocations. Either a
  tesswer fold, or runs on a history."
  ([]
   (->> (t/filter h/invoke?)
        (t/group-by :f)
        (t/into [])))
  ([history]
   (h/tesser history (invokes-by-f))))

(defn invokes-by-f-type
  "A fold which returns a map of f -> type -> ops, for all invocations."
  ([]
   (into (->> (t/filter h/invoke?)
              (t/group-by :f))
         (invokes-by-type)))
  ([history]
   (h/tesser history (invokes-by-f-type))))

(defn completions-by-f-type
  "Takes a history and returns a map of f -> type-> ops, for all completions in
  history."
  [history]
  (->> history
       (h/remove h/invoke?)
       (group-by :f)
       (util/map-kv (fn [[f ops]] [f (group-by :type ops)]))))

(defn rate
  "Map breaking down the mean rate of completions by f and type, plus totals at
  each level."
  [history]
  (->> history
       (h/remove h/invoke?)
       (reduce (fn [m op]
                 (let [f (:f op)
                       t (:type op)]
                   ; slow and bad
                   (-> m
                       (update-in [f t]         util/inc*)
                       (update-in [f ::all]     util/inc*)
                       (update-in [::all t]     util/inc*)
                       (update-in [::all ::all] util/inc*)))))))

(defn latency-point
  "Given an operation, returns a [time, latency] pair: times in seconds,
  latencies in ms."
  [op]
  (list (double (util/nanos->secs (:time op)))
        (double (util/nanos->ms   (:latency op)))))

(defn fs->points
  "Given a sequence of :f's, yields a map of f -> gnuplot-point-type, so we can
  render each function in a different style."
  [fs]
  (->> fs
       (map-indexed (fn [i f] [f (* 2 (+ 2 i))]))
       (into {})))

(defn qs->colors
  "Given a sequence of quantiles q, yields a map of q -> gnuplot-color, so we
  can render each latency curve in a different color."
  [qs]
  (-> qs
      sort
      reverse
      (zipmap (map vector
                   (repeat 'rgb)
                   (cycle ["red"
                           "orange"
                           "purple"
                           "blue"
                           "green"
                           "grey"])))))

(def types
  "What types are we rendering?"
  [:ok :info :fail])

(def type->color
  "Takes a type of operation (e.g. :ok) and returns a gnuplot color."
  {:ok   ['rgb "#81BFFC"]
   :info ['rgb "#FFA400"]
   :fail ['rgb "#FF1E90"]})

(defn nemesis-ops
  "Given a history and a nemeses specification, partitions the set of nemesis
  operations in the history into different nemeses, as per the spec. Returns
  the nemesis spec, restricted to just those nemeses taking part in this
  history, and with each spec augmented with an :ops key, which contains all
  operations that nemesis performed."
  [nemeses history]
  ; Build an index mapping :fs to nemeses.
  ; TODO: verify no nemesis fs overlap
  (assert (every? :name nemeses))
  (assert (= (distinct (map :name nemeses)) (map :name nemeses)))
  (let [index (->> nemeses
                   (map (fn [nemesis]
                          (zipmap (concat (:start nemesis   [:start])
                                          (:stop  nemesis   [:stop])
                                          (:fs    nemesis))
                                  (repeat (:name nemesis)))))
                   (reduce merge {}))
        ; Go through the history and build up a map of names to ops.
        ops-by-nemesis (->> history
                            (filter #(= :nemesis (:process %)))
                            (group-by (comp index :f)))
        ; And zip that together with the nemesis spec
        nemeses (keep (fn [n]
                        (when-let [ops (ops-by-nemesis (:name n))]
                          (assoc n :ops ops)))
                      nemeses)
        ; And add a default for any unmatched ops
        nemeses (if-let [ops (ops-by-nemesis nil)]
                  (conj nemeses {:name "nemesis"
                                 :ops  ops})
                  nemeses)]
    nemeses))

(defn nemesis-activity
  "Given a nemesis specification and a history, partitions the set of nemesis
  operations in the history into different nemeses, as per the spec. Returns
  the spec, restricted to just those nemeses taking part in this history, and
  with each spec augmented with two keys:

    :ops        All operations the nemeses performed
    :intervals  A set of [start end] paired ops."
  [nemeses history]
  (->> history
       (nemesis-ops nemeses)
       (map (fn [nemesis]
              (assoc nemesis :intervals
                     (util/nemesis-intervals (:ops nemesis) nemesis))))))

(defn interval->times
  "Given an interval of two operations [a b], returns the times [time-a time-b]
  covering the interval. If b is missing, yields [time-a nil]."
  [[a b]]
  [(double (util/nanos->secs (:time a)))
   (when b (double (util/nanos->secs (:time b))))])

(defn nemesis-regions
  "Given nemesis activity, emits a sequence of gnuplot commands rendering
  shaded regions where each nemesis was active. We can render a maximum of 12
  nemeses; this keeps size and spacing consistent."
  [plot nemeses]
  (->> nemeses
       (map-indexed
         (fn [i n]
           (let [color           (or (:fill-color n)
                                     (:color n)
                                     default-nemesis-color)
                 transparency    (:transparency n nemesis-alpha)
                 graph-top-edge  1
                 ; Divide our y-axis space into twelfths
                 height          0.0834
                 padding         0.00615
                 bot             (- graph-top-edge
                                    (* height (inc i)))
                 top             (+ bot height)]
             (->> (:intervals n)
                  (map interval->times)
                  (map (fn [[start stop]]
                         [:set :obj :rect
                          :from (g/list start [:graph (+ bot padding)])
                          :to   (g/list (or stop [:graph 1])
                                        [:graph (- top padding)])
                          :fillcolor :rgb color
                          :fillstyle :transparent :solid transparency
                          :noborder]))))))
       (reduce concat)))

(defn nemesis-lines
  "Given nemesis activity, emits a sequence of gnuplot commands rendering
  vertical lines where nemesis events occurred."
  [plot nemeses]
  (let [tfilter (if-let [[xmin xmax] (:xrange plot)]
                  (fn [t] (<= xmin t xmax))
                  identity)]
    (->> nemeses
         (mapcat (fn [n]
                   (let [color (or (:line-color n)
                                   (:color n)
                                   default-nemesis-color)
                         width (:line-width n "1")]
                     (->> (:ops n)
                          (map (comp double util/nanos->secs :time))
                          (filter tfilter)
                          (map (fn [t]
                                 [:set :arrow
                                  :from (g/list t [:graph 0])
                                  :to   (g/list t [:graph 1])
                                  :lc :rgb color
                                  :lw width
                                  :nohead])))))))))

(defn nemesis-series
  "Given nemesis activity, constructs the series required to show every present
  nemesis' activity in the legend. We do this by constructing dummy data, and a
  key that will match the way that nemesis's activity is rendered."
  [plot nemeses]
  (->> nemeses
       (map (fn [n]
              {:title     (str (:name n))
               :with      :lines
               :linecolor ['rgb (or (:fill-color n)
                                    (:color n)
                                    default-nemesis-color)]
               :linewidth 6
               :data      [[-1 -1]]}))))

(defn with-nemeses
  "Augments a plot map to render nemesis activity. Takes a nemesis
  specification: a collection of nemesis spec maps, each of which has keys:

    :name   A string uniquely naming this nemesis
    :color  What color to use for drawing this nemesis (e.g. \"#abcd01\")
    :start  A set of :f's which begin this nemesis' activity
    :stop   A set of :f's which end this nemesis' activity
    :fs     A set of :f's otherwise related to this nemesis"
  [plot history nemeses]
  (let [nemeses (nemesis-activity nemeses history)]
    (-> plot
        (update :series   concat (nemesis-series  plot nemeses))
        (update :preamble concat (nemesis-regions plot nemeses)
                                 (nemesis-lines   plot nemeses)))))

(defn preamble
  "Shared gnuplot preamble"
  [output-path]
  (concat [[:set :output output-path]
           [:set :term :png, :truecolor, :size (g/list 900 400)]]
          '[[set xlabel "Time (s)"]
            [set key outside top right]]))

(defn broaden-range
  "Given a [lower upper] range for a plot, returns [lower' upper'], which
  covers the original range, but slightly expanded, to fall nicely on integral
  boundaries."
  [[a b]]
  (if (= a b)
    ; If it's a zero-width interval, give it exactly 1 on either side.
    [(dec a) (inc a)]
    (let [; How big is the range?
          size (Math/abs (- (double b) (double a)))
          ; Divide the range into tenths
          grid (/ size 10)
          ; What's the nearest power of 10?
          scale (->> grid Math/log10 Math/round (Math/pow 10))
          ; Clamp
          a' (- a (mod a scale))
          b' (let [m (mod b scale)]
               ; If b is suuuuper close to the scale tick already...
               (if (< (/ m scale) 0.001)
                 b                                ; Just use b as is
                 (+ scale (- b (mod b scale)))))  ; Push b up to the next tick
          a' (min a a')
          b' (max b b')]
      [a' b'])))

(defn without-empty-series
  "Takes a plot, and strips out empty series objects."
  [plot]
  (update plot :series (partial filter (comp seq :data))))

(defn has-data?
  "Takes a plot and returns true iff it has at least one series with
  data points."
  [plot]
  (boolean (some (comp seq :data) (:series plot))))

(defn with-range
  "Takes a plot object. Where xrange or yrange are not provided, fills them in
  by iterating over each series :data."
  [plot]
  (let [data    (mapcat :data (:series plot))
        _       (when-not (seq data)
                  (throw+ {:type ::no-points
                           :plot plot}))
        [x0 y0] (first data)
        [xmin xmax ymin ymax] (reduce (fn [[xmin xmax ymin ymax] [x y :as pair]]
                                             [(min xmin x)
                                              (max xmax x)
                                              (min ymin y)
                                              (max ymax y)])
                                      [x0 x0 y0 y0]
                                      data)
        xrange (broaden-range [xmin xmax])
        yrange (if (= :y (:logscale plot))
                 ; Don't broaden logscale plots; we'll probably expand the
                 ; bounds to 0.
                 [ymin ymax]
                 (broaden-range [ymin ymax]))]
    (assoc plot
           :xrange (:xrange plot xrange)
           :yrange (:yrange plot yrange))))

(defn latency-preamble
  "Gnuplot commands for setting up a latency plot."
  [test output-path]
  (concat (preamble output-path)
          [[:set :title (str (:name test) " latency")]]
          '[[set ylabel "Latency (ms)"]
            [set logscale y]]))

(defn legend-part
  "Takes a series map and returns the list of gnuplot commands to render that
  series."
  [series]
  (remove nil?
          ["-"
           'with      (:with series)
           (when-let [t (:linetype series)]  ['linetype t])
           (when-let [c (:linecolor series)] ['linecolor c])
           (when-let [t (:pointtype series)] ['pointtype t])
           (when-let [w (:linewidth series)] ['linewidth w])
           (if-let [t (:title series)]       ['title t]       'notitle)]))

(defn plot!
  "Renders a gnuplot plot. Takes an option map:

    :preamble             Gnuplot commands to send first
    :series               A vector of series maps
    :draw-fewer-on-top?   If passed, renders series with fewer points on top
    :xrange               A pair [xmin xmax] which controls the xrange
    :yrange               Ditto, for the y axis
    :logscale             e.g. :y

  A series map is a map with:

    :data       A sequence of data points to render, e,g. [[0 0] [1 2] [2 4]]
    :with       How to draw this series, e.g. 'points
    :linetype   What kind of line to use
    :pointtype  What kind of point to use
    :title      A string, or nil, to label this series map
  "
  [opts]
  ; (info :plotting (with-out-str (pprint opts)))
  (assert (every? sequential? (map :data (:series opts)))
          (str "Series has no :data points\n"
               (with-out-str (pprint (remove (comp seq :data)
                                             (:series opts))))))
  (if (:draw-fewer-on-top? opts)
    ; We're going to transform our series in two ways: one, in their normal
    ; order, but with only a single dummy point, and second, those with the
    ; most points first, but without titles, so they don't appear in the
    ; legend.
    (let [series (:series opts)
          series (concat (->> series
                              (sort-by (comp count :data))
                              reverse
                              (map #(dissoc % :title)))
                         ; Then, the normal series, but with dummy points
                         (->> series
                              (map #(assoc % :data [[0 -1]]))))]
      ; OK, go ahead and render that
      (recur (assoc opts
                    :series series
                    :draw-fewer-on-top? false)))

    ; OK, normal rendering
    (let [series      (:series opts)
          preamble    (:preamble opts)
          xrange      (:xrange opts)
          yrange      (:yrange opts)
          logscale    (:logscale opts)
          ; The plot command
          plot        [['plot (apply g/list (map legend-part series))]]
          ; All commands
          commands    (cond-> []
                        preamble  (into preamble)
                        logscale  (conj [:set :logscale logscale])
                        xrange    (conj [:set :xrange (apply g/range xrange)])
                        yrange    (conj [:set :yrange (apply g/range yrange)])
                        true      (concat plot))
          ; Datasets
          data        (map :data series)]
      ; Go!
      ;(info (with-out-str
      ;        (pprint commands)
      ;        (pprint (map (partial take 2) data))))
      (try (g/raw-plot! commands data)
           (catch java.io.IOException e
             (throw (IllegalStateException. "Error rendering plot, verify gnuplot is installed and reachable" e)))))))

(defn point-graph!
  "Writes a plot of raw latency data points."
  [test history {:keys [subdirectory nemeses] :as opts}]
  (let [nemeses     (or nemeses (:nemeses (:plot test)))
        history     (util/history->latencies history)
        datasets    (invokes-by-f-type history)
        fs          (util/polysort (keys datasets))
        fs->points- (fs->points fs)
        output-path (.getCanonicalPath (store/path! test
                                                    subdirectory
                                                    "latency-raw.png"))
        preamble    (latency-preamble test output-path)
        series      (->> (for [f fs, t types]
                           (when-let [data (seq (get-in datasets [f t]))]
                             {:title     (str (util/name+ f) " " (name t))
                              :with      'points
                              :linetype  (type->color t)
                              :pointtype (fs->points- f)
                              :data      (map latency-point data)}))
                         (remove nil?))]
    (-> {:preamble           preamble
         :draw-fewer-on-top? true
         :logscale           :y
         :series             series}
        (with-range)
        (with-nemeses history nemeses)
        plot!
        (try+ (catch [:type ::no-points] _ :no-points)))))

(defn quantiles-graph!
  "Writes a plot of latency quantiles, by f, over time."
  [test history {:keys [subdirectory nemeses]}]
  (let [nemeses     (or nemeses (:nemeses (:plot test)))
        history     (util/history->latencies history)
        dt          30
        qs          [0.5 0.95 0.99 1]
        datasets    (->> history
                         invokes-by-f
                         ;; For each f, emit a map of quantiles to points
                         (util/map-kv
                          (fn [[f ops]]
                            (->> ops
                                 (map latency-point)
                                 (latencies->quantiles dt qs)
                                 (vector f)))))
        fs          (util/polysort (keys datasets))
        fs->points- (fs->points fs)
        qs->colors- (qs->colors qs)
        output-path (.getCanonicalPath
                     (store/path! test
                                  subdirectory
                                  "latency-quantiles.png"))

        preamble    (latency-preamble test output-path)
        series      (for [f fs, q qs]
                      {:title     (str (util/name+ f) " " q)
                       :with      'linespoints
                       :linetype  (qs->colors- q)
                       :pointtype  (fs->points- f)
                       :data      (get-in datasets [f q])})]
    (-> {:preamble preamble
         :series   series
         :logscale :y}
        (with-range)
        (with-nemeses history nemeses)
        plot!
        (try+ (catch [:type ::no-points] _ :no-points)))))

(defn rate-preamble
  "Gnuplot commands for setting up a rate plot."
  [test output-path]
  (concat (preamble output-path)
          [[:set :title (str (:name test) " rate")]]
          '[[set ylabel "Throughput (hz)"]]))

(defn rate-graph!
  "Writes a plot of operation rate by their completion times."
  [test history {:keys [subdirectory nemeses]}]
  (let [nemeses     (or nemeses (:nemeses (:plot test)))
        dt          10
        td          (double (/ dt))
        ; Times might technically be out-of-order (and our tests do this
        ; intentionally, just for convenience)
        t-max       (h/task history max-time []
                            (let [t (->> (t/map :time)
                                         (t/max)
                                         (h/tesser history))]
                              (util/nanos->secs (or t 0))))
        ; Compute rates: a map of f -> type -> time-bucket -> rate
        datasets
        (h/fold
          (->> history
               (h/remove h/invoke?)
               h/client-ops)
          (f/loopf {:name :rate-graph}
                   ; We work with a flat map for speed, and nest it at
                   ; the end
                   ([m (transient {})]
                    [^Op op]
                    (recur (let [bucket (bucket-time dt (util/nanos->secs
                                                          (.time op)))
                                 k [(.f op) (.type op) bucket]]
                             (assoc! m k (+ (get m k 0) td))))
                    (persistent! m))
                   ; Combiner: merge, then furl
                   ([m {}]
                    [m2]
                    (recur (merge-with + m m2))
                    (reduce (fn unfurl [nested [ks rate]]
                              (assoc-in nested ks rate))
                            {}
                            m))))
        fs          (util/polysort (keys datasets))
        fs->points- (fs->points fs)
        output-path (.getCanonicalPath
                      (store/path! test subdirectory "rate.png"))
        preamble (rate-preamble test output-path)
        series   (for [f fs, t types]
                   {:title     (str (util/name+ f) " " (name t))
                    :with      'linespoints
                    :linetype  (type->color t)
                    :pointtype (fs->points- f)
                    :data      (let [m (get-in datasets [f t])]
                                 (map (juxt identity #(get m % 0))
                                      (buckets dt @t-max)))})]
    (-> {:preamble  preamble
         :series    series}
        (with-range)
        (with-nemeses history nemeses)
        plot!
        (try+ (catch [:type ::no-points] _ :no-points)))))
