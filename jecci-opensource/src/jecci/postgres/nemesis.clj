(ns jecci.postgres.nemesis
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen
    [client :as client]
    [control :as c]
    [nemesis :as nemesis]
    [net :as net]
    [generator :as gen]
    [util :as util :refer [letr]]]
   [jepsen.nemesis.time :as nt]
   [jecci.postgres.db :as db]
   [jecci.utils.colors :as cr])
  (:use [slingshot.slingshot :only [throw+ try+]]))

; looks like start and stop must both be unique here
(def plot-spec
  {:nemeses #{{:name        "kill postmaster"
               :color       (cr/ret-rm-color)
               :start       #{:kill-postmaster}
               :stop        #{:start-pg}}
              {:name        "stop pg"
               :color       (cr/ret-rm-color)
               :start       #{:stop-pg}
               :stop        #{:resume-pg}}
              {:name        "kill checkpointer"
               :color       (cr/ret-rm-color)
               :start       #{:kill-checkpointer}
               :stop        #{:resume-checkpointer}}
              {:name        "kill autovacuum launcher"
               :color       (cr/ret-rm-color)
               :start       #{:kill-autovacuum-launcher}
               :stop        #{:resume-autovacuum-launcher}}
              {:name        "kill stats collector"
               :color       (cr/ret-rm-color)
               :start       #{:kill-stats-collector}
               :stop        #{:resume-stats-collector}}
              {:name        "kill logical replication launcher"
               :color       (cr/ret-rm-color)
               :start       #{:kill-logical-replication-launcher}
               :stop        #{:resume-logical-replication-launcher}}}})

(def nemesis-specs
  "These are the types of failures that the nemesis can perform."
  #{:kill-postmaster
    :kill-checkpointer
    :kill-autovacuum-launcher
    :kill-stats-collector
    :kill-logical-replication-launcher
    :stop-pg

    ;:clock-skew

    ; Special-case generators
    })

(def kill-dict
  {:kill-checkpointer "postgres: checkpointer"
   :kill-autovacuum-launcher "postgres: autovacuum launcher"
   :kill-stats-collector "postgres: stats collector"
   :kill-logical-replication-launcher "postgres: logical replication launcher"})

(def process-faults
  "Faults affecting individual processes"
  [:kill-postmaster :stop-pg])

(def autorecover-faults
  [:kill-checkpointer
   :kill-autovacuum-launcher
   :kill-stats-collector
   :kill-logical-replication-launcher])

(def network-faults
  "Faults affecting the network"
  [:partition])

(defn kill-postmaster!
  []
  (try+
   (info (c/exec "pkill" :-9 :-e :-c :-U "jecci" :-f "postgres -D"))
   (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(defn kill-pg-subprocess
  "kill the pg process greped by given string"
  [op]
  (try+
   (info (c/exec "pkill" :-9 :-e :-c :-U "jecci" :-f (op kill-dict)))
   (catch [:type :jepsen.control/nonzero-exit] _ nil)))

(def all-nemeses
  "All nemesis specs to run as a part of a complete test suite."
  (->> (concat
         ; No faults
        [[]]
         ; Single types of faults
        (map vector process-faults))
    ; Convert to maps like {:fault-type true}
       (map (fn [faults] (zipmap faults (repeat true))))))

(def quick-nemeses
  "All nemesis specs to run as a part of a complete test suite."
  (->> (concat
         ; No faults
        [[]]
         ; Single types of faults
        (map vector process-faults))
    ; Convert to maps like {:fault-type true}
       (map (fn [faults] (zipmap faults (repeat true))))))

(def very-quick-nemeses
  (->> (concat
        [[]]
        [(concat process-faults
                 autorecover-faults
                 network-faults)])
       (map (fn [faults] (zipmap faults (repeat true))))))

(defn prn-node-nemesis
  [node nemesis]
  (info node "doing" nemesis)
  node)

(defn process-nemesis
  "A nemesis that can pause, resume, start, stop, and kill processes."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [nodes (:nodes test)
            nodes (case (:f op)
                    ; When resuming, resume all nodes
                    (:start-pg :resume-pg) nodes

                    (util/random-nonempty-subset nodes))
            ; If the op wants to give us nodes, that's great
            nodes (or (:value op) nodes)]
        (assoc op :value
               (c/on-nodes test nodes
                           (fn [test node]
                             (case (:f op)
                               :start-pg  (db/pg-start! test node)
                               :resume-pg (db/pg-start! test node)
                               :kill-postmaster   (kill-postmaster!)
                               :stop-pg   (db/pg-stop! test node)))))))

    (teardown! [this test])))

(defn autorecover-nemesis
  "A nemesis that performs autorecover nemesis"
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [nodes (:nodes test)
            ; If the op wants to give us nodes, that's great
            nodes (or (:value op) nodes)]
        (assoc op :value
               (c/on-nodes test nodes
                           (fn [test node]
                             (case (:f op)
                               :kill-checkpointer (kill-pg-subprocess :kill-checkpointer)
                               :kill-autovacuum-launcher (kill-pg-subprocess :kill-autovacuum-launcher)
                               :kill-stats-collector (kill-pg-subprocess :kill-stats-collector)
                               :kill-logical-replication-launcher (kill-pg-subprocess :kill-logical-replication-launcher)
                               nil))))))
    (teardown! [_ _])))

; The mapper of actions to functions
(def nemesis-composition
  {#{:start-pg :resume-pg
     :kill-postmaster :stop-pg}    (process-nemesis)
   #{:kill-checkpointer :resume-checkpointer
     :kill-autovacuum-launcher :resume-autovacuum-launcher
     :kill-stats-collector :resume-stats-collector
     :kill-logical-replication-launcher :resume-logical-replication-launcher}  (autorecover-nemesis)
   ;{:reset-clock          :reset
   ; :strobe-clock         :strobe
   ; :check-clock-offsets  :check-offsets
   ; :bump-clock           :bump}          (nt/clock-nemesis)
   })

; The mapper of nemesis to healer
; will be used to build generators
(def op2heal-dict
  {:kill-postmaster :start-pg
   :stop-pg :resume-pg})

; The mapper of autorecover nemesis to stop sign
; When perform these nemeses, the db should auto recover
; the stop sign is just a symbol for plotter to plot
; will be used to build generators
(def autorecover-ops
  {:kill-checkpointer :resume-checkpointer
   :kill-autovacuum-launcher :resume-autovacuum-launcher
   :kill-stats-collector :resume-stats-collector
   :kill-logical-replication-launcher :resume-logical-replication-launcher})

; The mapper of nemesis to actions that will be executed eventually
; like finally in try catch
(def final-gen-dict
  {:kill-postmaster :start-pg
   :stop-pg :resume-pg})

(defn special-full-generator [_])
