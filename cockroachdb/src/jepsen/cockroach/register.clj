(ns jepsen.cockroach.register
  "Single atomic register test"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as cockroach]
                    [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [reconnect :as rc]
                    [independent :as independent]
                    [util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cockroach.client :as c]
            [jepsen.cockroach.nemesis :as cln]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord AtomicClient [tbl-created? conn]
  client/Client

  (setup! [this test node]
    (let [conn (c/client node)]
      (info node "Connected")
      ;; Everyone's gotta block until we've made the table.
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (c/with-conn [c conn]
            (Thread/sleep 1000)
            (j/execute! c ["drop table if exists test"])
            (Thread/sleep 1000)
            (info node "Creating table")
            (j/execute! c ["create table test (id int primary key, val
                           int)"]))))

      (assoc this :conn conn)))

  (invoke! [this test op]
    (c/with-idempotent #{:read}
      (c/with-exception->op op
        (c/with-conn [c conn]
          (c/with-timeout
            (try
              (c/with-txn [c c]
                (let [id   (key (:value op))
                      val' (val (:value op))
                      val  (-> c
                               (c/query ["select val from test where id = ?"
                                         id]
                                        {:row-fn :val
                                         :timeout c/timeout-delay})
                               first)]
                  (case (:f op)
                    :read (assoc op :type :ok, :value (independent/tuple id val))

                    :write (do
                             (if (nil? val)
                               (c/insert! c :test {:id id :val val'})
                               (c/update! c :test {:val val'} ["id = ?" id]))
                             (cockroach/update-keyrange! test :test id)
                             (assoc op :type :ok))

                    :cas (let [[expected-val new-val] val'
                               cnt (c/update! c :test {:val new-val}
                                              ["id = ? and val = ?"
                                               id expected-val])]
                           (assoc op :type (if (zero? (first cnt))
                                             :fail
                                             :ok))))))
              (catch org.postgresql.util.PSQLException e
                (if (re-find #"ERROR: restart transaction" (.getMessage e))
                  ; Definitely failed
                  (assoc op :type :fail)
                  (throw e)))))))))

  (teardown! [this test]
    (rc/close! conn)))

(defn test
  [opts]
  (cockroach/basic-test
    (merge
      {:name        "register"
       :client      {:client (AtomicClient. (atom false) nil)
                     :during (independent/concurrent-generator
                               10
                               (range)
                               (fn [k]
                                 (->> (gen/reserve 5 (gen/mix [w cas cas]) r)
                                      (gen/delay-til 1/2)
                                      (gen/stagger 0.1)
                                      (gen/limit 100))))}
       :model       (model/cas-register 0)
       :checker     (checker/compose
                      {:perf   (checker/perf)
                       :details (independent/checker
                                  (checker/compose
                                    {:timeline     (timeline/html)
                                     :linearizable (checker/linearizable)}))})}
      opts)))
