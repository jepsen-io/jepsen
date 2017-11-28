(ns aerospike.set
  "Uses CAS ops on a single key to add elements to a set"
  (:require [aerospike.support :as s]
            [clojure.string :as str]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]))

(defrecord SetClient [client namespace set key]
  client/Client
  (open! [this test node]
    (assoc this :client (s/connect node)))

  (setup! [this test])

  (invoke! [this test op]
    (s/with-errors op #{}
      (case (:f op)
        :read (assoc op
                     :type :ok,
                     :value (-> client
                                (s/fetch namespace set key)
                                :bins
                                :value
                                (str/split #" ")
                                (->> (map #(Long/parseLong %))
                                     (into (sorted-set)))))

        :add (do (try (s/cas! client namespace set key
                              (fn [record]
                                {:value (str (:value record) " " (:value op))}))
                      (catch clojure.lang.ExceptionInfo e
                        (if (= (.getMessage e) "cas not found")
                          (s/put-if-absent! client namespace set key
                                            {:value (str (:value op))}))))
                 (assoc op :type :ok)))))

  (teardown! [this test])

  (close! [this test]
    (s/close client)))

(defn set-client
  "A set on top of a single key and bin"
  []
  (SetClient. nil s/ans "cats" "meow"))

(defn workload
  []
  {:client  (set-client)
   :checker (checker/compose
              {:timeline (timeline/html)
               :set      (checker/set)})
   :generator (->> (range)
                   (map (fn [x] {:type :invoke, :f :add, :value x}))
                   gen/seq
                   (gen/stagger 1/10))
   :final-generator (gen/each (gen/once {:type :invoke, :f :read}))})
