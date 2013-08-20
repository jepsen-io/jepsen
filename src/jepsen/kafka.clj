(ns jepsen.kafka
  "Kafka test."
  (:require [clj-kafka.consumer.zk  :as consumer]
            [clj-kafka.producer     :as producer]
            [clj-kafka.zk           :as zk]
            [clj-kafka.core         :as kafka]
            [jepsen.codec           :as codec])
  (:use
    jepsen.util
    jepsen.set-app
    jepsen.load))

(defn drain!
  "Returns a sequence of all elements available within dt millis."
  [opts topic dt]
  (kafka/with-resource
    [consumer (consumer/consumer
                {"zookeeper.connect"   (str (:host opts) ":2181")
                 "group.id"            "jepsen.consumer"
                 "auto.offset.reset"   "smallest"
                 "auto.commit.enable"  "true"})]
    consumer/shutdown
    (let [done (atom [])]
      (loop [coll (consumer/messages consumer [topic])]
        (if (-> done
                (swap! conj (first coll))
                future
                (deref dt ::timeout)
                (= ::timeout))
          @done
          (recur (rest coll)))))))

(defn kafka-app
  [opts]
  (let [enqueued (atom 0)
        topic "jepsen"
        broker (->> (zk/brokers
                      {"zookeeper.connect" (str (:host opts) ":2181")})
                    (filter #(= (:host %) (:host opts)))
                    first)
        producer (producer/producer
                   {"metadata.broker.list" (str (:host opts) ":9092")
                    "serializer.class"     "kafka.serializer.DefaultEncoder"
                    "partitioner.class"    "kafka.producer.DefaultPartitioner"})]

    (reify SetApp
      (setup [app]
        (teardown app))

      (add [app element]
        (try
          (->> element
               codec/encode
               (producer/message "jepsen")
               (producer/send-message producer))
          ok
          (catch org.apache.zookeeper.KeeperException$ConnectionLossException e
            error)))

      (results [app]
        (->> (drain! opts topic 1000)
             (map :value)
             (map codec/decode)))

      (teardown [app]
        (prn "Teardown: " (map (comp codec/decode :value)
                               (drain! opts topic 1000)))))))
