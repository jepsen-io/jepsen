(ns jepsen.checker.perf
  "Supporting functions for performance analysis."
  (:require [clojure.stacktrace :as trace]
            [fipp.edn :refer [pprint]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.util :as util]
            [jepsen.store :as store]
            [multiset.core :as multiset]
            [gnuplot.core :as g]
            [knossos.core :as knossos]
            [knossos.op :as op]
            [knossos.history :as history]))

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
  looking at their corresponding completions."
  [ops]
  {:ok   (filter #(= :ok   (:type (:completion %))) ops)
   :fail (filter #(= :fail (:type (:completion %))) ops)
   :info (filter #(= :info (:type (:completion %))) ops)})

(defn invokes-by-f
  "Takes a history and returns a map of f -> ops, for all invocations."
  [history]
  (->> history
       (filter op/invoke?)
       (group-by :f)))

(defn invokes-by-f-type
  "Takes a history and returns a map of f -> type -> ops, for all invocations."
  [history]
  (->> history
       (filter op/invoke?)
       (group-by :f)
       (util/map-kv (fn [[f ops]] [f (invokes-by-type ops)]))))

(defn completions-by-f-type
  "Takes a history and returns a map of f -> type-> ops, for all completions in
  history."
  [history]
  (->> history
       (remove op/invoke?)
       (group-by :f)
       (util/map-kv (fn [[f ops]] [f (group-by :type ops)]))))

(defn rate
  "Map breaking down the mean rate of completions by f and type, plus totals at
  each level."
  [history]
  (->> history
       (r/remove op/invoke?)
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

(defn nemesis-intervals
  "Given a history, constructs a sequence of [start-time, stop-time] intervals
  when the nemesis was active, in units of seconds."
  ([history]
   (nemesis-intervals history {}))
  ([history opts]
   (let [final-time (-> history
                        rseq
                        (->> (filter :time))
                        first
                        :time
                        (or 0)
                        util/nanos->secs
                        double)
         history (util/nemesis-intervals history opts)]
     (keep (fn [interval]
             (let [[start stop] interval]
               (when start
                 [(-> start :time util/nanos->secs double)
                  (if stop
                    (-> stop :time util/nanos->secs double)
                    final-time)])))
           history))))

(defn nemesis-regions*
  "Emits a sequence of gnuplot commands rendering shaded regions where the
  nemesis is active. We can render a maximum of 12 nemeses; this keeps nemesis
  size and spacing consistent.

  {:name \"Your Nemesis Here (TM)\"
   :start #{:start1 :start2}
   :stop #{:stop1 :stop2}
   :fill-color \"#000000\"
   :transparency 0.05}

  :name must be provided for the nemesis to be displayed in the legend."
  ([history]
   (nemesis-regions* history {}))
  ([history opts]
   (let [fill-color     (or (:fill-color   opts) default-nemesis-color)
         transparency   (or (:transparency opts) nemesis-alpha)
         history        (nemesis-intervals history opts)
         graph-top-edge 1
         ;; Divide our y-axis space into twelfths
         height         0.0834
         padding        0.00615
         idx            (inc (or (:idx opts) 0))
         bot            (- graph-top-edge
                           (* height idx))
         top            (+ bot height)]
     (->> history
        (map (fn [[start stop]]
               [:set :obj :rect
                :from (g/list start [:graph (+ bot padding)])
                :to   (g/list stop  [:graph (- top padding)])
                :fillcolor :rgb fill-color
                :fillstyle :transparent :solid transparency
                :noborder]))))))

(defn active-nemeses
  "Given a history and nemesis specs, returns just those which actually
  participated in this history."
  [history nemeses]
  ; Build a map of fs to nemesis specifications
  (let [index (->> nemeses
                   (map (fn [nemesis]
                          (zipmap (concat (:start nemesis) (:stop nemesis))
                                  (repeat nemesis))))
                   (reduce merge {}))]
    ; Figure out which nemeses are present
    (->> history
         (filter #(= :nemesis (:process %)))
         (map :f)
         (keep index)
         distinct
         (sort-by name))))

(defn nemesis-regions
  "Wraps nemesis-regions* to work with collections of nemeses."
  [history nemeses]
  (let [nemeses (active-nemeses history nemeses)
        c       (count nemeses)
        f       (fn [idx nemesis]
                  (let [nemesis (assoc nemesis
                                       :idx idx
                                       :total c)]
                    (nemesis-regions* history nemesis)))
        x       (map-indexed f nemeses)]
    (apply concat x)))

(defn nemesis-events
  "Given a history, constructs a sequence of times, in seconds, marking nemesis
  events other than start/stop pairs.

  Nemesis operations may happen significantly later than the last operation in
  the history, but we typically only size our plots relative to the data we're
  plotting in the history. To prevent drawing outside the plot region, we
  constrain our nemesis events to those before the final client :invoke op."
  [history opts]
  (let [start (or (:start opts) #{:start})
        stop  (or (:stop  opts) #{:stop})
        tmax  (or (first-time (filter op/invoke? (rseq history))) 0)]
    (->> history
         (remove (fn [op]
                   (and (not= :nemesis (:process op))
                        (not (start (:f op)))
                        (not (stop  (:f op))))))
         (map (comp double util/nanos->secs :time))
         (take-while (partial >= tmax)))))

(defn nemesis-lines
  "Emits a sequence of gnuplot commands rendering vertical lines where nemesis
  events occurred.

  Takes an options map for nemesis regions and styling ex:
  {:name \"Your Nemesis Here (TM)\"
   :start #{:start1 :start2}
   :stop #{:stop1 :stop2}
   :line-color #\"dddddd\"
   :line-width 1}"
  ([history]
   (nemesis-lines history {}))
  ([history opts]
   (let [events      (nemesis-events history opts)
         line-color  (or (:line-color opts) default-nemesis-color)
         line-width  (or (:line-width opts) "1")]
     (map (fn [t]
            [:set :arrow
             :from (g/list t [:graph 0])
             :to   (g/list t [:graph 1])
             ;; When gnuplot gets alpha rgb we can use this
             ;; :lc :rgb "#f3000000"
             :lc :rgb line-color
             :lw line-width
             :nohead])
          events))))

(defn nemesis-series
  "Given a history and a specification for nemeses, constructs the series
  required to show every present nemesis' activity in the legend. We do this by
  constructing dummy data, and a key that will match the way that nemesis's
  activity is rendered."
  [history nemeses]
  (->> (active-nemeses history nemeses)
       ; Convert to series
       (map (fn [n]
              {:title     (:name n)
               :with      :lines
               :linecolor ['rgb (:fill-color n default-nemesis-color)]
               :linewidth 6
               :data      [[-1 -1]]}))))

(defn preamble
  "Shared gnuplot preamble"
  [output-path]
  (concat [[:set :output output-path]
           [:set :term :png, :truecolor, :size (g/list 900 400)]]
          '[[set autoscale]
            [set xlabel "Time (s)"]
            [set key outside top right]]))

(defn xrange
  "Computes the xrange for a history. Optionally, filters the history with f."
  ([history]
   (xrange identity history))
  ([f history]
  (let [xmin (or (first-time (filter f history))        (g/lit "*"))
        xmax (or (first-time (filter f (rseq history))) (g/lit "*"))]
    (g/range xmin xmax))))

(defn latency-preamble
  "Gnuplot commands for setting up a latency plot."
  [test output-path]
  (concat (preamble output-path)
          [[:set :title (str (:name test) " latency")]
           [:set :xrange (xrange op/invoke? (:history test))]]
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

  A series map is a map with:

    :data       A sequence of data points to render, e,g. [[0 0] [1 2] [2 4]]
    :with       How to draw this series, e.g. 'points
    :linetype   What kind of line to use
    :pointtype  What kind of point to use
    :title      A string, or nil, to label this series map
  "
  [opts]
  ; (info :plotting (with-out-str (pprint opts)))
  (assert (every? seq (map :data (:series opts)))
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
    (let [series     (:series opts)
          ; The plot command
          plot        [['plot (apply g/list (map legend-part series))]]
          ; All commands
          commands    (concat (:preamble opts) plot)
          ; Datasets
          data        (map :data series)]
      ; Go!
      ;(pprint commands)
      ;(pprint (map (partial take 2) data))
      (try (g/raw-plot! commands data)
           (catch java.io.IOException _
             (throw (IllegalStateException. "Error rendering plot, verify gnuplot is installed and reachable")))))))

(defn with-nemeses
  "Augments a plot map to render nemesis activity"
  [plot test history nemeses]
  (let [nemeses     (or nemeses #{{}})
        nem-regions (nemesis-regions history nemeses)
        nem-lines   (mapcat #(nemesis-lines history %) nemeses)
        series      (nemesis-series history nemeses)]
    (-> plot
        (update :preamble concat nem-regions nem-lines)
        (update :series concat series))))

(defn point-graph!
  "Writes a plot of raw latency data points."
  [test history {:keys [subdirectory nemeses] :as opts}]
  (let [nemeses     (or nemeses (:nemeses (:plot test)))
        history     (util/history->latencies history)
        datasets    (invokes-by-f-type history)
        fs          (util/polysort (keys datasets))
        fs->points  (fs->points fs)
        output-path (.getCanonicalPath (store/path! test
                                                    subdirectory
                                                    "latency-raw.png"))
        preamble    (latency-preamble test output-path)
        series      (->> (for [f fs, t types]
                           (when-let [data (seq (get-in datasets [f t]))]
                             {:title     (str (util/name+ f) " " (name t))
                              :with      'points
                              :linetype  (type->color t)
                              :pointtype (fs->points f)
                              :data      (map latency-point data)}))
                         (remove nil?))]
    (-> {:preamble           preamble
         :draw-fewer-on-top? true
         :series             series}
        (with-nemeses test history nemeses)
        plot!)
    output-path))

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
        fs->points  (fs->points fs)
        qs->colors  (qs->colors qs)
        output-path (.getCanonicalPath
                     (store/path! test
                                  subdirectory
                                  "latency-quantiles.png"))

        preamble    (latency-preamble test output-path)
        series      (for [f fs, q qs]
                      {:title     (str (util/name+ f) " " q)
                       :with      'linespoints
                       :linetype  (qs->colors q)
                       :pointtype  (fs->points f)
                       :data      (get-in datasets [f q])})]
    (-> {:preamble preamble
         :series   series}
        (with-nemeses test history nemeses)
        plot!)))

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
        t-max       (->> history (r/map :time) (reduce max 0) util/nanos->secs)
        datasets    (->> history
                         (r/remove op/invoke?)
                         ; Don't graph nemeses
                         (r/filter (comp integer? :process))
                         ; Compute rates
                         (reduce (fn [m op]
                                   (update-in m [(:f op)
                                                 (:type op)
                                                 (bucket-time dt
                                                              (util/nanos->secs
                                                               (:time op)))]
                                              #(+ td (or % 0))))
                                 {}))
        fs          (util/polysort (keys datasets))
        fs->points  (fs->points fs)
        output-path (.getCanonicalPath (store/path! test
                                                    subdirectory
                                                    "rate.png"))

        preamble (rate-preamble test output-path)
        series   (for [f fs, t types]
                   {:title     (str (util/name+ f) " " (name t))
                    :with      'linespoints
                    :linetype  (type->color t)
                    :pointtype (fs->points f)
                    :data      (let [m (get-in datasets [f t])]
                                 (map (juxt identity #(get m % 0))
                                      (buckets dt t-max)))})]
    (-> {:preamble  preamble
         :series    series}
        (with-nemeses test history nemeses)
        plot!)))
