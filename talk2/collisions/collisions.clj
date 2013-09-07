(use 'incanter.distributions
     'clojure.math.numeric-tower)

(defn timestamps
  "Returns a lazy, infinite sequence of double timestamps with inter-timestamp
  intervals governed by the given distribution."
  [dist]
  (->> dist
       (partial draw)
       repeatedly
       (reductions + 0)))

(defn poisson-timestamps
  "Returns a lazy, infinite sequence of double timestamps for a poisson process
  with the given mean rate, in seconds."
  [rate]
  (timestamps (exponential-distribution rate)))

(defn gamma-timestamps
  [shape rate]
  (timestamps (gamma-distribution shape rate)))

(defn microsecond
  "Converts a time in seconds to long microseconds."
  [t]
  (-> t (* 1000000) long))

(defn collisions
  "Counts collisions in a sorted timestamp series."
  [ts]
  (->> ts
       (reduce (fn [[cs prev] cur]
                 (if (= prev cur)
                   [(inc cs) ::fencepost]
                   [cs cur]))
               [0 ::fencepost])
       first))

(defn collision-p
  "Given a sequence of timestamps, estimates the probability of a collision on
  any given write."
  [ts]
  (let [[colls cnt] (reduce (fn [[colls cnt prev] cur]
                              (if (= prev cur)
                                [(inc colls) (inc cnt) cur]
                                [colls (inc cnt) cur]))
                            [0 0 ::fencepost]
                            ts)]
    (/ colls cnt)))

(defn collision-fraction
  "The fraction of time during which a collision holds, assuming synchronized
  clocks."
  [ts]
  (let [[_ _ bad-time good-time]
        (reduce (fn [[prev state bad-time good-time] cur]
                  [cur
                   (if (= prev cur)
                     :bad
                     :good)
                   (if (= :bad state)
                     (+ bad-time (- cur prev))
                     bad-time)
                   (if (= :good state)
                     (+ good-time (- cur prev))
                     good-time)
                   cur])
                [nil nil 0 0]
                ts)]
    (/ bad-time (+ bad-time good-time))))

(defn observance-p
  "Probability of not observing an event with probability p, given n
  observations:"
  [n p]
  (- 1 (expt (- 1 p) n)))

(defn explore
  "Given a timestamp generator function, a reduction function to apply to a
  timestamp series and a sequence of parameters, returns a map of parameters to
  estimates of the reduced value over that generated series."
  [generator reduction params]
  (->> params
       (pmap (fn [params]
               [params (->> (if (sequential? params)
                              (apply generator params)
                              (generator params))
                            (take 1000000)
                            (map microsecond)
                            reduction
                            double)]))
       (into {})))

(defn map-vals
  [f m]
  (->> m
       (map (fn [[k v]] [k (f v)]))
       (into {})))

; Probability of a read seeing a collision for various poisson rates
(->> (range 0 7)
     (map (partial expt 10))
     (explore poisson-timestamps collision-fraction)
     clojure.pprint/pprint)

; Probability of observing no collisions in a day, at 1 read per second
(->> (range 0 7)
     (map (partial expt 10))
     (explore poisson-timestamps collision-fraction)
     (map-vals (partial observance-p (* 3600 24))) 
     clojure.pprint/pprint)

; Probability of a collision on any given write.
(->> (range 0 3)
     (map (partial expt 10))
     (explore poisson-timestamps collision-p)
     clojure.pprint/pprint)
