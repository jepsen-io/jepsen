(ns elasticsearch.sets
  (:require [cheshire.core          :as json]
            [clojure.java.io        :as io]
            [clojure.string         :as str]
            [clojure.tools.logging  :refer [info]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.checker.timeline  :as timeline]
            [jepsen.control.net       :as net]
            [jepsen.control.util      :as cu]
            [jepsen.os.debian         :as debian]
            [clj-http.client          :as http]
            [clojurewerkz.elastisch.rest          :as es]
            [clojurewerkz.elastisch.rest.admin    :as esa]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.index    :as esi]
            [clojurewerkz.elastisch.rest.response :as esr]))

; Insert elements into an index to implement a set
(defrecord CreateSetClient [client]
  client/Client
  (setup! [_ test node]
    (let [client (await-client node)]
      ;; Create index
      (try
        (esi/create client index-name
                    :mappings {"number" {:properties
                                         {:num {:type "integer"
                                                :store "yes"}}}}
                    :settings {"index" {"refresh_interval" "1s"}})
        (catch clojure.lang.ExceptionInfo e
          (when-not (index-already-exists-error? e)
            (throw e))))
      (esa/cluster-health client
                          {:index [index-name] :level "indices"
                           :wait_for_status "green"
                           :wait_for_nodes (count (:nodes test))})
      (CreateSetClient. client)))

  (invoke! [this test op]
      (case (:f op)
        :add (timeout 5000
               (assoc op :type :info :value :timed-out)
               (try
                 (let [r (esd/create client index-name "number" {:num (:value op)})]
                   (cond (esr/ok? r)
                         (assoc op :type :ok)

                         (esr/timed-out? r)
                         (assoc op :type :info :value :timed-out)

                         :else
                         (assoc op :type :info :value r)))
                 (catch java.net.SocketException e
                   (assoc op :type :info :value (.getMessage e)))
                 (catch java.net.ConnectException e
                   (assoc op :type :fail :value (.getMessage e)))))
        :read (try
                (info "Waiting for recovery before read")
                ; Elasticsearch lies about cluster state during split brain
                ; woooooooo
                (c/on-many (:nodes test) (wait 1000 :green))
                (info "Elasticsearch reports green, but it's probably lying, so waiting another 10s")
                (Thread/sleep (* 10 1000))
                (info "Recovered; flushing index before read")
                (esi/flush client index-name)
                (assoc op :type :ok
                       :value (->> (esd/search client index-name "number"
                                               :scroll "10s"
                                               :size 20)
                                   (esd/scroll-seq client)
                                   (map (comp :num :_source))
                                   (into (sorted-set))))
                (catch RuntimeException e
                  (assoc op :type :fail :value (.getMessage e))))))

  (teardown! [_ test]
    (.close client)))

(defn create-set-client
  "A set implemented by creating independent documents"
  []
  (CreateSetClient. nil))

; Use ElasticSearch MVCC to do CAS read/write cycles, implementing a set.
(defrecord CASSetClient [mapping-type doc-id client]
  client/Client
  (setup! [_ test node]
    (let [client (await-client node)]
      ;; Create index
      (try
        (esi/create client index-name
                    :mappings {mapping-type {:properties {}}})
        (catch clojure.lang.ExceptionInfo e
          (when-not (index-already-exists-error? e)
            (throw e))))
      (esa/cluster-health client
                          {:index [index-name] :level "indices"
                           :wait_for_status "green"
                           :wait_for_nodes (count (:nodes test))})

      ; Initial empty set
      (esd/create client index-name mapping-type {:values []} :id doc-id)

      (CASSetClient. mapping-type doc-id client)))

  (invoke! [this test op]
    (case (:f op)
        :add (timeout 5000 (assoc op :type :info :value :timed-out)
                      (let [current (esd/get client index-name mapping-type doc-id
                                                :preference "_primary")]
                        (if-not (esr/found? current)
                          ; Can't write without a read
                          (assoc op :type :fail)

                          (let [version (:_version current)
                                values  (-> current :_source :values)
                                values' (vec (conj values (:value op)))
                                r       (esd/put client index-name mapping-type doc-id
                                                 {:values values'}
                                                 :version version)]
                            (cond ; lol esr/ok? actually means :found, and
                                  ; esr/found? means :ok or :found, and
                                  ; *neither* of those appear in a successful
                                  ; conditional put, so I hope this is how
                                  ; you're supposed to interpret responses.
                                  (esr/conflict? r)
                                  (assoc op :type :fail)

                                  (esr/timed-out? r)
                                  (assoc op :type :info :value :timed-out)

                                  :else (assoc op :type :ok))))))

        :read (try
                (info "Waiting for recovery before read")
                (c/on-many (:nodes test) (wait 200 :green))
                (info "Recovered; flushing index before read")
                (esi/flush client index-name)
                (assoc op :type :ok
                       :value (->> (esd/get client index-name mapping-type doc-id
                                       :preference "_primary")
                                   :_source
                                   :values
                                   (into (sorted-set)))))))

  (teardown! [_ test]
    (.close client)))

(defn cas-set-client []
  (CASSetClient. "cas-sets" "0" nil))

(defn adds
  "Generator that emits :add operations for sequential integers."
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn recover
  "A generator which stops the nemesis and allows some time for recovery."
  []
  (gen/nemesis
    (gen/phases
      (gen/once {:type :info, :f :stop})
      (gen/sleep 20))))

(defn read-once
  "A generator which reads exactly once."
  []
  (gen/clients
    (gen/once {:type :invoke, :f :read})))


(defn create-test
  "A generic create test."
  [name opts]
  (es-test (str "create " name)
           (merge {:client  (create-set-client)
                   :model   (model/set)
                   :checker (checker/compose
                              {:set  checker/set
                               :perf (checker/perf)})}
                  opts)))

(defn create-isolate-primaries-test
  "Inserts docs into a set while isolating all primaries with a partition."
  []
  (create-test "isolate primaries"
               {:nemesis   isolate-self-primaries-nemesis
                :generator (gen/phases
                             (->> (adds)
                                  (gen/stagger 1/10)
                                  (gen/delay 1)
                                  (gen/nemesis
                                    (gen/seq (cycle
                                               [(gen/sleep 30)
                                                {:type :info :f :start}
                                                (gen/sleep 200)
                                                {:type :info :f :stop}])))
                                  (gen/time-limit 800))
                             (recover)
                             (read-once))}))

(defn create-pause-test
  "Inserts docs into a set while pausing random primaries with SIGSTOP/SIGCONT."
  []
  (create-test "pause"
               {:nemesis   (nemesis/hammer-time
                             (comp rand-nth self-primaries) "java")
                :generator (gen/phases
                             (->> (adds)
                                  (gen/stagger 1/10)
                                  (gen/delay 1)
                                  (gen/nemesis
                                    (gen/seq (cycle
                                               [(gen/sleep 10)
                                                {:type :info :f :start}
                                                (gen/sleep 120)
                                                {:type :info :f :stop}])))
                                  (gen/time-limit 600))
                             (recover)
                             (read-once))}))

(defn create-crash-test
  "Inserts docs into a set while killing random nodes and restarting them."
  []
  (create-test "crash"
               {:nemesis   crash-nemesis
                :generator (gen/phases
                             (->> (adds)
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                    (gen/seq (cycle
                                               [(gen/sleep 1)
                                                {:type :info :f :start}
                                                (gen/sleep 1)
                                                {:type :info :f :stop}])))
                                  (gen/time-limit 600))
                             (recover)
                             (read-once))}))

(defn create-bridge-test
  "Inserts docs into a set while weaving the network into happy little
  intersecting majority rings"
  []
  (create-test "bridge"
               {:nemesis   (nemesis/partitioner (comp nemesis/bridge shuffle))
                :generator (gen/phases
                             (->> (adds)
                                  (gen/stagger 1/10)
                                  (gen/delay 1)
                                  (gen/nemesis
                                    (gen/seq (cycle
                                               [(gen/sleep 10)
                                                {:type :info, :f :start}
                                                (gen/sleep 120)
                                                {:type :info, :f :stop}])))
                                  (gen/time-limit 600))
                             (recover)
                             (read-once))}))
