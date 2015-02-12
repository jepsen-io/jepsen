(ns jepsen.console
  "Formats ANSI art for console logging."
  (:use jepsen.util)
  (:require [clansi.core :as clansi]
            [clojure.set :as set]))

(def full       \█)
(def horizontal [nil \▏ \▎ \▍ \▌ \▋ \▊ \▉ \█])
(def vertical   [nil \▁ \▂ \▃ \▄ \▅ \▆ \▇ \█])

(defn whole
  "What's the number of whole characters (floored) in f * w?"
  [f w]
  (int (Math/floor (* f w))))

(defn eighths
  "What's the number of fractional eighth characters (rounded up) in f * w?"
  [f w]
  (let [exact (* f w)
        fractional (- exact (whole f w))]
    (int (Math/ceil (* fractional 8)))))

(defn horizontal-bar
  "Returns a horizontal bar covering fraction f of w chars."
  [f w]
  (apply str (concat (repeat (whole f w) full)
                     (list (horizontal (eighths f w))))))

(defn colorize-ranges
  "Colorizes ranges of a string, like a VU meter. Takes a string, followed by a
  color and the maximum character index to apply it to.
  
  (colorize-ranges \"12345678\" :green 5 :yellow 6 :red 7)"
  [s & ranges]
  (let [ranges (if (even? (count ranges))
                 (concat ranges (list :default (.length s)))
                 (concat ranges (list (.length s))))]
    (->> ranges
         (partition 2)
         (reduce (fn [[strips i1] [color i2]]
                   (if (< (.length s) i1)
                     (reduced [strips i1])
                     [(conj strips
                            (clansi/style
                              (.substring s i1 (min i2 (.length s)))
                              color))
                      i2]))
                 [[] 0])
         first
         (apply str))))

(defn latency-bar
  "A colorized latency bar chart."
  [latency]
  (let [ok       1000
        warning  5000
        critical 10000
        w 60
        m 30000]
  (str
    (clansi/style (int latency) (condp < latency
                                  critical :red
                                  warning  :yellow
                                  ok       :cyan
                                  :green))
    \tab
    (-> (/ latency m)
        (horizontal-bar w)
        (colorize-ranges :green     (* w (/ ok m))
                         :cyan      (* w (/ warning m))
                         :yellow    (* w (/ critical m))
                         :red)))))

(defn line
  "A log line for a response."
  [{:keys [req state latency message]}]
  (str req \tab
       (clansi/style
         state
         (case state
           :ok :green
           :error :red
           :cyan))
       \tab
       (latency-bar latency)
       (when message
         (str "\n" message))))

(defn wrap-log
  "Returns a function that calls (f req), then logs the request and a colorized
  bar representing the request latency."
  [f]
  (fn logger [req]
    (let [res (f req)]
      (log (line res))
      res)))

(def ordered-log-buffer 
  "An atom wrapping [next req index, pending responses, ready-to-print]"
  (atom [0
         (sorted-set-by #(compare (:req %1)
                                  (:req %2)))
         []]))

(defn wrap-ordered-log
  "Like wrap-log, but ensures responses are printed in request order."
  [f]
  (fn logger [req]
    (let [res (f req)]
      (locking ordered-log-buffer
        (->> (swap! ordered-log-buffer
                    (fn [[idx queued _]]
                      (let [queued (conj queued res)
                            [idx ready] (reduce (fn [[idx ready] e]
                                                  (if (= idx (:req e))
                                                    [(inc idx) (conj ready e)]
                                                    (reduced
                                                      [idx ready])))
                                                [idx []]
                                                queued)
                            queued (apply disj queued ready)]
                        [idx queued ready])))
             last
             (map (comp log line))
             dorun)
        res))))
