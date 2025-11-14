(ns jepsen.checker.plot
  "Draws plots as a part of the checker. This namespace should
  eventually subsume jepsen.checker.perf."
  (:require [clojure.stacktrace :as trace]
            [fipp.edn :refer [pprint]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
						[dom-top.core :refer [loopr]]
            [gnuplot.core :as g]
            [jepsen [history :as h]
                    [store :as store]
                    [util :as util :refer [nanos->secs]]]
            [jepsen.history.fold :as f]
						[jepsen.checker.perf :as perf]
            [multiset.core :as multiset]
            [slingshot.slingshot :refer [try+ throw+]]
            [tesser.core :as t])
  (:import (jepsen.history Op)))

(defn op-color-plot-points
  "Helps `op-color-plot!` by constructing a map of group names to vectors of [x
  y] points. Takes a test, history, the groups map and group-fn, and the window
  count."
  [test history groups group-fn window-count]
  (h/ensure-pair-index history)
  (let [t0          (:time (first history))
        t1          (:time (peek history))
        dt          (- t1 t0)
        window-size (/ dt window-count)
        ; A vector of windows, where each window is a map of {group name to an
        ; empty vector of times. We'll derive the y coordinates once the
        ; windows are built.
        empty-window  (update-vals groups (constantly []))
        empty-windows (vec (repeat window-count empty-window))
        windows
        (->> ;(t/take 10)
             (t/filter h/invoke?)
             (t/fold
               {:identity (constantly empty-windows)
                :reducer  (fn reducer [windows op]
                            (let [window (-> (:time op)
                                             (- t0)
                                             (/ dt)
                                             (* window-count)
                                             Math/floor
                                             long)
                                  value (:value op)
                                  group (group-fn op)]
                              (if (nil? group)
                                windows
                                (update windows window
                                        update group
                                        conj (nanos->secs (:time op))))))
                :combiner (fn combiner [windows1 windows2]
                            (mapv (fn merge [win1 win2]
                                    (merge-with into win1 win2))
                                  windows1
                                  windows2))})
             (h/tesser history))
        ; How many points in the biggest window?
        max-window-count (->> windows
                              (map (fn c [window]
                                     (reduce + (map count (vals window)))))
                              (reduce max 0))
        ; How many Hz is the biggest window?
        max-window-hz (/ max-window-count (nanos->secs window-size))]
    ; Now for each group we'll unfurl each window into [x y] points, where the
    ; y coordinates are scaled relative to the window with the most points.
    ; This a.) spreads out points, and b.) means the y axis of the graph gives
    ; a feeling for overall throughput over time.
    (loopr [series empty-window]
           [window windows]
           ; With this window, give each point an increasing counter, carrying
           ; that counter i across all groups.
           (recur
             (loopr [i      0
                     series series]
                    [[group times] window
                     t            times]
                    (let [y (float (* max-window-hz (/ i max-window-count)))]
                      (recur (inc i)
                             (update series group conj [t y])))
                    series)))))

(defn op-color-plot!
  "Renders a timeseries plot of each operation in a history, where operations
  are partitioned into a few distinct groups. Each operation is shown as a
  single point with its horizontal position given by invocation time, and with
  its vertical position splayed out such that the overall height of a 'pile' of
  points roughly shows the overall throughput at that time. The color of each
  point is given by the group. Nemesis activity is also shown, as with perf
  plots.

  Takes a test, a history, and an options map. Options are:

  :groups       A map of group names to maps defining how that group works.
                Default: {:ok {:color \"#81BFFC\"}
                          :info {:color \"#FFA400\"}
                          :fail {:color \"#FF1E90\"}}
  :group-fn     A function (f invoke) which takes an invocation to a group name.
                Defaults to `(:type (history/completion history invoke))`.
                If group-fn returns `nil`, that operation is skipped.
  :title        The title of the plot. Default: '<test-name> ops'.
  :filename     The filename to write. Default: 'op-color-plot.png'
  :window-count The number of time windows to divide the history into. Default
                512.

  The options map can include these common checker options too:

  :subdirectory Optionally, the subdirectory of the store dir to write to.
  :nemeses      Descriptions of how to render nemeses, as for perf plots.
                Defaults to (:nemeses (:plot test))."
  [test history opts]
  (let [groups       (:groups opts
                              {:ok   {:color "#81BFFC"}
                               :info {:color "#FFA400"}
                               :fail {:color "#FF1E90"}})
        group-fn     (:group-fn opts
                                (fn group-fn [invoke]
                                  (.type (h/completion history invoke))))
        title        (:title opts (str (:name test) " ops"))
        filename     (:filename opts "op-color-plot.png")
        window-count (:window-count opts 512)
        nemeses      (:nemeses opts (:nemeses (:plot test)))
        datasets     (op-color-plot-points test history groups group-fn
                                        window-count)
        output       (.getCanonicalPath
                       (store/path! test (:subdirectory opts) filename))
        preamble     (concat (perf/preamble output)
                             [['set 'title title]
                              '[set ylabel "Throughput (Hz)"]])
        series (for [[group points] datasets]
                 {:title     (name group)
                  :with      'points
                  :linetype  ['rgb (:color (groups group) "#666666")]
                  ; With few points, use bigger dots
                  :pointtype  (if (< (count points) 16384) 1 0)
                  :data       points})]
  	(prn :ready)
    (-> {:preamble preamble, :series series}
        perf/with-range
        (perf/with-nemeses history nemeses)
        perf/plot!
        (try+ (catch [:type ::no-points] _ :no-points)))))
