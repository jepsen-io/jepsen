(ns aerospike.core
  "Entry point for aerospike tests"
  (:require [aerospike [support :as support]
                       [counter :as counter]
                       [cas-register :as cas-register]
                       [set :as set]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.os.debian :as debian])
  (:gen-class))

(defn workloads
  "The workloads we can run. Each workload is a map like

      {:generator         a generator of client ops
      :final-generator   a generator to run after the cluster recovers
      :client            a client to execute those ops
      :checker           a checker
      :model             for the checker}"
  []
  {:cas-register (cas-register/workload)
   :counter      (counter/workload)
   :set          (set/workload)})

(defn aerospike-test
  "Constructs a Jepsen test map from CLI options."
  [opts]
  (let [{:keys [generator
                final-generator
                client
                checker
                model]} (get (workloads) (:workload opts))
        generator (->> generator
                       (gen/nemesis (gen/start-stop 10 10))
                       (gen/time-limit (:time-limit opts)))
        generator (if-not final-generator
                    generator
                    (gen/phases generator
                                (gen/log "Healing cluster")
                                (gen/nemesis
                                  (gen/once {:type :info, :f :stop}))
                                (gen/log "Waiting for quiescence")
                                (gen/sleep 10)
                                (gen/clients final-generator)))]
    (merge tests/noop-test
           opts
           {:name     (str "aerospike " (name (:workload opts)))
            :os       debian/os
            :db       (support/db)
            :client   client
            :nemesis  (nemesis/partition-majorities-ring)
            :generator generator
            :checker  (checker/compose
                      {:perf (checker/perf)
                       :workload checker})
            :model    model})))

(def opt-spec
  "Additional command-line options"
  [[nil "--workload WORKLOAD" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of (workloads)))
    :validate [(workloads) (cli/one-of (workloads))]]])

(defn -main
  "Handles command-line arguments, running a Jepsen command."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   aerospike-test
                                         :opt-spec  opt-spec})
                   (cli/serve-cmd))
            args))
