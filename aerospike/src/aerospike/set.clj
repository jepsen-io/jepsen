(ns aerospike.set
  "Uses CAS ops on a single key to add elements to a set"
  (:require [aerospike.support :as s]
            [clojure.string :as str]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]))

(defrecord SetClient [client namespace set]
  client/Client
  (open! [this test node]
    (assoc this :client (s/connect node)))

  (setup! [this test])

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (s/with-errors op #{}
        (case (:f op)
          :read (assoc op
                       :type :ok,
                       :value (independent/tuple k
                                (-> client
                                   (s/fetch namespace set k)
                                   :bins
                                   :value
                                   (or "")
                                   (str/split #" ")
                                   (->> (remove str/blank?)
                                        (map #(Long/parseLong %))
                                        (into (sorted-set))))))

          :add (do (s/append! client namespace set k {:value (str " " v)})
                   (assoc op :type :ok))))))

  (teardown! [this test])

  (close! [this test]
    (s/close client)))

(defn set-client
  "A set on top of a single key and bin"
  []
  (SetClient. nil s/ans "cats"))

(defn workload
  []
  (let [max-key (atom 0)]
    {:client  (set-client)
     :checker (independent/checker (checker/set))
     :generator (independent/concurrent-generator
                  5
                  (range)
                  (fn [k]
                    (swap! max-key max k)
                    (->> (range 10000)
                         (map (fn [x] {:type :invoke, :f :add, :value x}))
                         gen/seq
                         (gen/stagger 1/10))))
     :final-generator (gen/derefer
                        (delay
                          (locking keys
                            (independent/concurrent-generator
                              5
                              (range (inc @max-key))
                              (fn [k]
                                (gen/stagger 10
                                   (gen/each
                                     (gen/once {:type :invoke
                                                :f    :read}))))))))}))
