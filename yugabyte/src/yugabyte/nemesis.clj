(ns yugabyte.nemesis
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]
             [util :as util :refer [meh timeout]]]
            [jepsen.nemesis.time :as nt]
            [slingshot.slingshot :refer [try+]]
            [yugabyte.auto :as auto]))

(defn process-nemesis
  "A nemesis that can start, stop, and kill randomly selected subsets of
  nodes."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [nodes (:nodes test)
            nodes (case (:f op)
                    (:start-master :start-tserver) nodes

                    (:stop-tserver :kill-tserver)
                    (util/random-nonempty-subset nodes)

                    (:stop-master :kill-master)
                    (util/random-nonempty-subset (auto/master-nodes test)))
            db (:db test)]
        (assoc op :value
               (c/on-nodes test nodes
                           (fn [test node]
                             (case (:f op)
                               :start-master  (auto/start-master!   db test)
                               :start-tserver (auto/start-tserver!  db test)
                               :stop-master   (auto/stop-master!    db)
                               :stop-tserver  (auto/stop-tserver!   db)
                               :kill-master   (auto/kill-master!    db)
                               :kill-tserver  (auto/kill-tserver!   db)))))))

    (teardown! [this test])))

(defn full-nemesis
  "Merges together all nemeses"
  []
  (nemesis/compose
    {#{:start-master :start-tserver
       :stop-master  :stop-tserver
       :kill-master  :kill-tserver}   (process-nemesis)
     {:start-partition :start
      :stop-partition  :stop}         (nemesis/partitioner nil)
     {:reset-clock          :reset
      :strobe-clock         :strobe
      :check-clock-offsets  :check-offsets
      :bump-clock           :bump}    (nt/clock-nemesis)}))

; Generators

(defn op
  "Shorthand for constructing a nemesis op"
  ([f]
   (op f nil))
  ([f v]
   {:type :info, :f f, :value v})
  ([f v & args]
   (apply assoc (op f v) args)))

(defn partition-one-gen
  "A generator for a partition that isolates one node."
  [test process]
  (op :start-partition
     (->> test :nodes nemesis/split-one nemesis/complete-grudge)
     :partition-type :single-node))

(defn partition-half-gen
  "A generator for a partition that cuts the network in half."
  [test process]
  (op :start-partition
      (->> test :nodes shuffle nemesis/bisect nemesis/complete-grudge)
      :partition-type :half))

(defn partition-ring-gen
  "A generator for a partition that creates overlapping majority rings"
  [test process]
  (op :start-partition
      (->> test :nodes nemesis/majorities-ring)
      :partition-type :ring))

(defn bump-gen
  "Randomized clock bump generator. On random subsets of nodes, bumps the clock
  from -max-skew to +max-skew milliseconds, exponentially distributed."
  [max-skew-ms test process]
  (let [gen (nt/bump-gen test process)]
    (assoc gen :value
           (->> (:value gen)
                (map (fn [x] [(key x) (-> x val (* max-skew-ms) (quot 262144))]))
                (into {})))))

(defn clock-gen
  "Emits a random schedule of clock skew operations up to skew-ms milliseconds."
  [max-skew-ms]
  (gen/mix (concat [nt/reset-gen] (repeat 3 (partial bump-gen max-skew-ms)))))

(defn full-generator
  "Takes a nemesis options map `n`, and constructs a generator for all nemesis
  operations. This generator is used during normal nemesis operations."
  [n]
  (->> (cond-> []
         (:kill-tserver n)
         (conj (op :kill-tserver) (op :start-tserver))

         (:kill-master n)
         (conj (op :kill-master) (op :start-master))

         (:stop-tserver n)
         (conj (op :stop-tserver) (op :start-tserver))

         (:stop-master n)
         (conj (op :stop-master) (op :start-master))

         (:partition-one n)
         (conj partition-one-gen (op :stop-partition))

         (:partition-half n)
         (conj partition-half-gen (op :stop-partition))

         (:partition-ring n)
         (conj partition-ring-gen (op :stop-partition))

         (:clock-skew n)
         (conj (->> (nt/clock-gen)
                    (gen/f-map {:check-offsets  :check-clock-offsets
                                :reset          :reset-clock
                                :strobe         :strobe-clock
                                :bump           :bump-clock}))))
       gen/mix
       (gen/stagger (:interval n))))

(defn final-generator
  "Takes a nemesis options map `n`, and constructs a generator to stop all
  problems. This generator is called at the end of a test, before final client
  operations."
  [n]
  (->> (cond-> []
         (:clock-skew n)                          (conj :reset-clock)
         (or (:kill-tserver n) (:stop-tserver n)) (conj :start-tserver)
         (or (:kill-master n)  (:stop-master n))  (conj :start-master)

         (some n [:partition-one :partition-half :partition-ring])
         (conj :stop-partition))
       (map op)
       gen/seq))

(defn expand-options
  "We support shorthand options in nemesis maps, like :kill, which expands to
  both :kill-tserver and :kill-master. This function expands those."
  [n]
  (cond-> n
    (:kill n) (assoc :kill-tserver true
                     :kill-master true)
    (:stop n) (assoc :stop-tserver true
                     :kill-master true)
    (:partition n) (assoc :partition-one true
                          :partition-half true
                          :partition-ring true)))

(defn nemesis
  "Composite nemesis and generator, given test options."
  [opts]
  (let [n (expand-options (:nemesis opts))]
    {:nemesis         (full-nemesis)
     :generator       (full-generator n)
     :final-generator (final-generator n)}))
