(ns tidb.nemesis
  "Nemeses for TiDB"
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nemesis]
             [net :as net]
             [generator :as gen]
             [reconnect :as rc]
             [util :as util :refer [letr]]]
            [jepsen.nemesis.time :as nt]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [tidb.db :as db]
            [clojure.tools.logging :refer :all]))

;; duration between interruptions
(def nemesis-delay 5) ; seconds

;; duration of an interruption
(def nemesis-duration 5) ; seconds

;;;;;;;;;;;;;;;;;;; Common definitions ;;;;;;;;;;;;;;;;;;;;;;

(defn nemesis-no-gen
  []
  {:during gen/void
   :final gen/void})

(defn nemesis-single-gen
  []
  {:during (gen/seq (cycle [(gen/sleep nemesis-delay)
                            {:type :info, :f :start}
                            (gen/sleep nemesis-duration)
                            {:type :info, :f :stop}]))
   :final (gen/once {:type :info, :f :stop})})

(defn nemesis-double-gen
  []
  {:during (gen/seq (cycle [(gen/sleep nemesis-delay)
                            {:type :info, :f :start1}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :start2}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :stop1}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :stop2}
                            (gen/sleep nemesis-delay)
                            {:type :info, :f :start2}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :start1}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :stop2}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :stop1}
                            ]))
   :final (gen/seq [{:type :info, :f :stop1}
                    {:type :info, :f :stop2}])})

(defn compose
  "Takes a collection of nemesis maps, each with a :name, :during and :final
  generators, and a :client. Creates a merged nemesis map, with a generator
  that emits a mix of operations destined for each nemesis, and a composed
  nemesis that maps those back to their originals."
  [nemeses]
  (let [nemeses (remove nil? nemeses)]
    (assert (distinct? (map :names nemeses)))
    (let [; unwrap :f [name, inner] -> :f inner
          nemesis (->> nemeses
                       (map (fn [nem]
                              (let [my-name (:name nem)]
                                ; Function that selects our specific ops
                                [(fn f-select [[name f]]
                                   (when (= name my-name)
                                     (assert (not (nil? f)))
                                     f))
                                 (:client nem)])))
                       (into {})
                       ((fn [x] (pprint [:nemesis-map x]) x))
                       nemesis/compose)
          ; wrap :f inner -> :f [name, inner]
          during (->> nemeses
                      (map (fn [nemesis]
                             (let [gen  (:during nemesis)
                                   name (:name nemesis)]
                               (reify gen/Generator
                                 (op [_ test process]
                                   (when-let [op (gen/op gen test process)]
                                     (update op :f (partial vector name))))))))
                      gen/mix)
          final (->> nemeses
                     (map (fn [nemesis]
                            (let [gen  (:final nemesis)
                                  name (:name nemesis)]
                              (reify gen/Generator
                                (op [_ test process]
                                   (when-let [op (gen/op gen test process)]
                                     (update op :f (partial vector name))))))))
                     (apply gen/concat))]
      {:name   (str/join "+" (map :name nemeses))
       :clocks (reduce #(or %1 %2) (map :clocks nemeses))
       :client nemesis
       :during during
       :final  final})))

;;;;;;;;;;;;;;;;;;;;;;;;;;; Nemesis definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; empty nemesis
(defn none
  []
  (merge (nemesis-no-gen)
         {:name "blank"
          :client nemesis/noop
          :clocks false}))

;; random partitions
(defn parts
  []
  (merge (nemesis-single-gen)
  {:name "parts"
   :client (nemesis/partition-random-halves)
   :clocks false}))

;; start/stop server
(defn startstop
  [n]
  (merge (nemesis-single-gen)
         {:name (str "startstop" (if (> n 1) n ""))
          :client (nemesis/hammer-time
                    (comp (partial take n) shuffle) (nth [db/pdbin db/tikvpin db/tidbbin] (rand-int 3)))
          :clocks false}))

(defn startkill
  [n]
  (merge (nemesis-single-gen)
         {:name (str "startkill" (if (> n 1) n ""))
          :client (nemesis/node-start-stopper (comp (partial take n) shuffle)
                                              db/stop!
                                              db/quickstart!)
          :clocks false}))

;; majorities ring
(defn majring
  []
  (merge (nemesis-single-gen)
         {:name "majring"
          :client (nemesis/partition-majorities-ring)
          :clocks false}))

(defn slowing
  "Wraps a nemesis. Before underlying nemesis starts, slows the network by dt
  s. When underlying nemesis resolves, restores network speeds."
  [nem dt]
  (reify client/Client
    (setup! [this test node]
      (net/fast! (:net test) test)
      (client/setup! nem test node)
      this)

    (invoke! [this test op]
      (case (:f op)
        :start (do (net/slow! (:net test) test {:mean (* dt 1000) :variance 1})
                   (client/invoke! nem test op))

        :stop (try (client/invoke! nem test op)
                   (finally
                     (net/fast! (:net test) test)))

        (client/invoke! nem test op)))

    (teardown! [this test]
      (net/fast! (:net test) test)
      (client/teardown! nem test))))
