(ns jepsen.random
  "Pluggable generation of random values.

  ## Pluggable

  First, randomness should be pluggable. In normal tests, standard Clojure
  `(rand-int)` and friends are just fine. But in tests, it's nice if those can
  be replaced by a deterministic seed. When running in a hypervisor like
  Antithesis, you want to draw entropy from a special SDK, so it can
  intentionally send you down interesting paths.

  ## Fast

  Second, it should be *reasonably* fast. We'd like to ideally generate ~100 K
  ops/sec, and each operation might need to draw, say, 10 random values, which
  is 1 M values/sec. Basic Clojure (rand-int 5) on my ThreadRipper takes ~37
  ns. Clojure's data.generators takes ~35 ns. Our thread-local implementation,
  backed by a LXM splittable random, takes just ~33 ns.

  ## Thread-safe

  We want everyone in the Jepsen universe to be able to draw random values from
  this namespace without coordinating. This implies generating values should be
  thread-safe.

  ## Stateful

  This namespace must be stateful. We'd like callers to simply be able to call
  `(r/int 5)` and get 2.

  Pure, splittable random seeds ala `test.check` are nice, but they a.) come
  with a performance penalty, and b.) require threading that random state
  through essentially every function call and return. This is not only complex,
  but adds additional destructuring overhead at each call boundary.

  The main advantage of stateful random generators is determinism across
  threads, but this is not a major concern in Jepsen. In normal test runs, we
  don't care about reproducibility. In Antithesis, the entire thread schedule
  is deterministic, so we're free to share state across threads and trust that
  Antithesis Will Take Care Of It. In tests, we're generally drawing entropy
  from a single thread. It'd be *nice* to have thread-safe random generators,
  but it's not critical.

  ## Determinism

  In single-threaded contexts, we want to be able to seed randomness and have
  reproducible tests. Doing this across threads is not really important--if we
  were being rigorous we could thread a splittable random down through every
  thread spawned, but there's a LOT of threaded code in Jepsen and it doesn't
  all know about us. More to the point, our multithreaded code is usually a.)
  non-random, or b.) doing IO, which we can't control. Having determinism for a
  single thread gets us a reasonable 'bang for our buck'.

  ## Special Distributions

  Jepsen needs some random things that aren't well supported by the normal
  java.util.Random, clojure.core, or data.generators functions. In particular,
  we like to do:

  1. Zipfian distributions: lots of small things, but sometimes very
     large things.
  2. Weighted choices: 90% reads, 5% writes, 5% deletes.
  3. Special values: over-represent maxima, minima, and zero, to stress
     codepaths that might treat them differently.

  ## Usage

  To re-bind randomness to a specifically seeded RNG, use:

  (jepsen.random/with-seed 5
    (jepsen.random/long)                  ; Returns the same value every time
    (call-stuff-using-jepsen.random ...)  ; This holds for the whole body

  This changes a global variable `jepsen.random/rng` and is NOT THREAD SAFE. Do
  not use `with-seed` concurrently. It's fine to spawn threads within the body,
  but if those threads are spawned in a nondeterministic order, then their
  calls to jepsen.random will also be nondeterministic.
  "
  (:refer-clojure :exclude [double long rand-nth shuffle])
  (:require [clj-commons.primitive-math :as p]
            [clojure.core :as c]
            [clojure.data.generators :as dg]
            [dom-top.core :refer [loopr]]
            [potemkin :refer [definterface+]])
  (:import (java.util.random RandomGenerator
                             RandomGenerator$SplittableGenerator
                             RandomGeneratorFactory
                             )))

;; Implementations. We provide a few implementations, mainly for performance
;; comparison purposes. Each random implementation satisfies a subset of the
;; the Java RandomGenerator API (nextDouble and nextLong), AND is also expected
;; to be threadsafe.

; Calls Clojure's built-ins
(deftype ClojureRandom []
  RandomGenerator
  (nextDouble [r]
    (rand))
  (nextDouble [r upper]
    (rand upper))
  (nextDouble [r lower upper]
    (p/+ lower (c/double (rand (p/- upper lower)))))

  (nextLong [r]
    (rand-int Long/MAX_VALUE))
  (nextLong [r upper]
    (rand-int upper))
  (nextLong [r lower upper]
    (p/+ lower (c/long (rand-int (p/- upper lower))))))

(defn clojure-random
  "A random source that uses Clojure's built-ins."
  []
  (ClojureRandom.))

; Calls data.generators
(deftype DataGeneratorsRandom []
  RandomGenerator
  (nextDouble [r]
    (dg/double))
  (nextDouble [r upper]
    (p/* (dg/double) upper))
  (nextDouble [r lower upper]
    (p/+ lower (* (dg/double) (p/- upper lower))))

  (nextLong [r]
    (dg/uniform))
  (nextLong [r upper]
    (dg/uniform 0 upper))
  (nextLong [r lower upper]
    (dg/uniform lower upper)))

(defn data-generators-random
  "A random source that uses data.generators."
  []
  (DataGeneratorsRandom.))

; Our implementation, which uses a RandomGenerator stored in a
; ThreadLocal. See https://openjdk.org/jeps/356 and https://prng.di.unimi.it/
(deftype ThreadLocalRandom [^ThreadLocal local]
  RandomGenerator
  (nextLong [r]
    (.nextLong ^RandomGenerator (.get local)))
  (nextLong [r lower upper]
    (.nextLong ^RandomGenerator (.get local) lower upper)))

(defn thread-local-random
  "Constructs a ThreadLocalRandom. Right now we use an L64X128MixRandom, and
  split off a fresh instance for every thread that asks for entropy."
  ([] (thread-local-random nil))
  ([seed]
   (let [; First, we need to make an RNG. We use the LXM family because it's
         ; splittable, so we can generate one for each thread.
         rngf (RandomGeneratorFactory/of "L64X128MixRandom")
         _    (assert (not (.isDeprecated rngf)))
         ^RandomGenerator$SplittableGenerator root-rng
         (if seed
           (.create rngf (c/long seed))
           (.create rngf))
         ; And make a ThreadLocal that takes RNGs from the root
         thread-local
         (proxy [ThreadLocal] []
           (initialValue []
             (locking root-rng
               (.split root-rng))))]
     (ThreadLocalRandom. thread-local))))

(def ^RandomGenerator rng
  "This is our default implementation of randomness. We don't use a dynamic var
  here because it turns out to add significant overhead (~30 ns), roughly
  halving speed, and because when you're choosing a different random
  implementation, you want it *everywhere*, not just in one thread tree.

  To select a different random generator, use

      (with-redefs [r/rng (r/thread-local-random some-seed)]
        ...)"
  (thread-local-random))

(defmacro with-rng
  "Evaluates body with the given random number generator. Not thread-safe;
  takes effect globally."
  [rng & body]
  `(with-redefs [rng ~rng] ~@body))

(defmacro with-seed
  "Evaluates body with the random generator re-bound to a determistic PRNG with
  the given seed. Not thread-safe; takes effect globally."
  [seed & body]
  `(with-rng (thread-local-random ~seed) ~@body))

; Basic uniform values

(defn long
  "Generates a uniformly distributed primitive long in [lower, upper)"
  (^long [] (.nextLong rng))
  (^long [^long upper]
         (.nextLong rng upper))
  (^long [^long lower, ^long upper] (.nextLong rng lower upper)))

(defn double
  "Generates a uniformly distributed double in [lower, upper)."
  (^double [] (.nextDouble rng))
  (^double [^double upper] (.nextDouble rng upper))
  (^double [^double lower, ^double upper]
           ; Fun fact: at least with L64X128MixRandom...
           ; rng.nextDouble(Double/MIN_VALUE, 0) throws "bound must be greater than origin"
           ; rng.nextDouble(Double/MIN_VALUE, 1.0) returns values in [0, 1)
           ; rng.nextDouble(Double/MIN_VALUE, Double/MAX_VALUE) returns values
           ; around 10^305 -- 10^308.
           ; I don't know how to go about reporting or fixing this
           (.nextDouble rng lower upper)))

;; Nonuniform distributions

(defn exp
  "Generates a exponentially distributed random double with rate parameter
  lambda."
  [lambda]
  (* (Math/log (p/- 1.0 (double))) (- lambda)))

(defn geometric
  "Generates a geometrically distributed random long with probability p.
  Specifically, these approximate the number of independent Bernoulli trial
  failures before the first success, supported on 0, 1, 2, .... We do this
  because--as with all our discrete distributions--we often want to use these
  values as indices into vectors, which are zero-indexed."
  [^double p]
  (-> (Math/log (double))
      (/ (Math/log (- 1.0 p)))
      Math/ceil
      c/long
      dec))

(def zipf-default-skew
  "When we choose zipf-distributed things, what skew do we generally pick? The
  algorithm has a singularity at 1, so we choose a slightly larger value."
  1.0001)

(defn zipf-b-inverse-cdf
  "Inverse cumulative distribution function for the zipfian bounding function
  used in `zipf`."
  (^double [^double skew ^double t ^double p]
           (let [tp (* t p)]
             (if (<= tp 1)
               ; Clamp so we don't fly off to infinity
               tp
               (Math/pow (+ (* tp (- 1 skew))
                            skew)
                         (/ (- 1 skew)))))))

(defn zipf
  "Selects a Zipfian-distributed integer in [0, n) with a given skew factor.
  Adapted from the rejection sampling technique in
  https://jasoncrease.medium.com/rejection-sampling-the-zipf-distribution-6b359792cffa.

  Note that the standard Zipfian ranges from (0, n], but you almost always want
  to use a Zipfian with a zero-indexed collection, so we emit [0, n) instead.
  Add one if you want the standard Zipfian."
  (^long [^long n]
   (zipf zipf-default-skew n))
  (^long [^double skew ^long n]
   (if (= n 0)
     0
     (do (assert (not= 1.0 skew)
                 "Sorry, our approximation can't do skew = 1.0! Try a small epsilon, like 1.0001")
         (let [t (/ (- (Math/pow n (- 1 skew)) skew)
                    (- 1 skew))]
           (loop []
             (let [inv-b         (zipf-b-inverse-cdf skew t (double))
                   sample-x      (c/long (+ 1 inv-b))
                   y-rand        (double)
                   ratio-top     (Math/pow sample-x (- skew))
                   ratio-bottom  (/ (if (<= sample-x 1)
                                      1
                                      (Math/pow inv-b (- skew)))
                                    t)
                   rat (/ ratio-top (* t ratio-bottom))]
               (if (< y-rand rat)
                 (- sample-x 1)
                 (recur)))))))))

; Choosing things from collections

(defn rand-nth
  "Like clojure's rand-nth, but uses our RNG."
  [coll]
  (let [c (count coll)]
    (when (= 0 c)
      (throw (IndexOutOfBoundsException.)))
    (nth coll (long c))))

(defn rand-nth-empty
  "Like rand-nth, but returns `nil` for empty collections."
  [coll]
  (try (rand-nth coll)
       (catch IndexOutOfBoundsException e nil)))

(defn double-weighted-index
  "Takes a total weight and an array of double weights for a weighted discrete
  distribution. Generates a random long index into those weights, with
  probability proportionate to that index's weight. Returns -1 when
  total-weight is 0.

  Note that `total-weight` is optional and should always be (reduce + weights).
  It's only an argument so you can sum once, rather than every call."
  (^long [^doubles weights]
   (double-weighted-index (reduce + weights) weights))
  (^long [^double total-weight ^doubles weights]
   (if (== 0.0 total-weight)
     -1
     (let [r (double 0 total-weight)]
       ; Linear search through weights
       (loop [i   0
              sum 0.0]
         (let [sum' (p/+ sum (aget weights i))]
           (if (p/< r sum')
             i
             (recur (inc i) sum'))))))))

(defn weighted-fn
  "Like weighted-choice, but returns a *function* which, when invoked with no
  arguments, returns a random value. This version is significantly faster than
  weighted-choice, as it can pre-compute key values."
  [wcs]
  (let [c       (count wcs)
        weights (double-array c)
        choices (object-array c)]
    ; Fill in weights and choices arrays based on choices structure
    (cond
      ; Map of choice -> weight
      (map? wcs)
      (loopr [i 0]
             [[choice weight] wcs]
             (do (aset-double weights i weight)
                 (aset choices i choice)
                 (recur (inc i))))

      ; Unknown
      true (throw (IllegalArgumentException.
                    (str "weighted-choice-fn expects a map, not a "
                         (type choices)
                         ": " (pr-str choices)))))
    (let [^double total-weight (areduce weights i ret 0.0
                                        (+ ret (aget weights i)))]
      (fn chooser []
        (let [i (double-weighted-index total-weight weights)]
          (aget choices i))))))

(defn weighted
  "Picks a random selection out of several weighted choices. Takes a map of
  values to weights, like so:

    {:foo 5
     :bar 3
     :baz 2}

  This returns :foo half of the time, :bar 30% of the time, and :baz 20% of the
  time. This is slow; see weighted-choice-fn for a faster variant."
  [choices]
  ((weighted-fn choices)))

(defn shuffle
  "Like Clojure shuffle, but based on our RNG. We use a Fisher-Yates shuffle
  here; see
  http://en.wikipedia.org/wiki/Fisher–Yates_shuffle#The_modern_algorithm"
  [coll]
  (let [buffer (object-array coll)]
    (loop [i (dec (alength buffer))]
      (if (< 0 i)
        (let [j   (long 0 (inc i))
              tmp (aget buffer i)]
          (aset buffer i (aget buffer j))
          (aset buffer j tmp)
          (recur (dec i)))
        (into (empty coll) (seq buffer))))))

(defn nonempty-subset
  "A non-empty prefix of a permutation of the given collection. Lengths are
  uniformly distributed. This is technically not a subset; if there are
  duplicates in the input collection, there may be duplicates in the output;
  nor is the output a set. Still, we use this constantly for subset-like
  things, like 'take a random collection of some nodes from the test'.

  Returns nil if collection is empty. Otherwise, returns at least singleton
  prefices, and up to full permutations of coll itself."
  [coll]
  (let [c (count coll)]
    (when (< 0 c)
      (take (inc (long c)) (shuffle coll)))))
