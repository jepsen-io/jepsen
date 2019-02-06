(ns yugabyte.core
  "Integrates workloads, nemeses, and automation to construct test maps."
  (:require [clojure.tools.logging :refer :all]
            [jepsen [generator :as gen]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os [debian :as debian]
                       [centos :as centos]]
            [yugabyte [auto :as auto]
                      [nemesis :as nemesis]]))

(defn yugabyte-ssh-defaults
  "A partial test map with SSH options for a test running in Yugabyte's
  internal testing environment."
  []
  {:ssh {:port                     54422
         :strict-host-key-checking false
         :username                 "yugabyte"
         :private-key-path (str (System/getenv "HOME")
                                "/.yugabyte/yugabyte-dev-aws-keypair.pem")}})

(def trace-logging
  "Logging configuration for the test which sets up traces for queries."
  {:logging {:overrides {;"com.datastax"                            :trace
                         ;"com.yugabyte"                            :trace
                         "com.datastax.driver.core.RequestHandler" :trace
                         ;"com.datastax.driver.core.CodecRegistry"  :info
                         }}})

(defn yugabyte-test
  [opts]
  (let [{:keys [client-generator
                client-final-generator]} opts
        generator (->> client-generator
                       (gen/nemesis (nemesis/gen opts))
                       (gen/time-limit (:time-limit opts)))
        generator (if-not client-final-generator
                    generator
                    (gen/phases
                     generator
                     (gen/log "Healing cluster")
                     (gen/nemesis (nemesis/final-gen opts))
                     (gen/log "Waiting for quiescence")
                     (gen/sleep 30)
                     (gen/clients client-final-generator)))]
  (merge tests/noop-test
         (dissoc opts :client-generator :client-final-generator)
         (when (:yugabyte-ssh opts) (yugabyte-ssh-defaults))
         (when (:trace-cql opts)    trace-logging)
         {:db         (case (:db opts)
                        :community-edition  (auto/community-edition)
                        :enterprise-edition (auto/enterprise-edition))
          :os         (case (:os opts)
                        :centos centos/os
                        :debian debian/os)
          :generator  generator
          :nemesis    (nemesis/get-nemesis-by-name (:nemesis opts))
          :max-clock-skew-ms (nemesis/get-nemesis-max-clock-skew-ms opts)})))
