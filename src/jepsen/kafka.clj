(ns jepsen.kafka
  "Kafka test."
  (:require [clj-kafka.consumer.zk  :as consumer]
            [clj-kafka.producer     :as producer]
            [clj-kafka.zk           :as zk]
            [clj-kafka.core         :as kafka]
            [clojure.string         :as string]
            [jepsen.failure         :as failure]
            [jepsen.control         :as control]
            [jepsen.control.net     :as control.net]
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

(defn app
  [opts]
  (let [enqueued (atom 0)
        topic "jepsen"
        broker (->> (zk/brokers
                      {"zookeeper.connect" (str (:host opts) ":2181")})
                    (filter #(= (:host %) (:host opts)))
                    first)
        producer (producer/producer
                   {"metadata.broker.list" (str (:host opts) ":9092")
                    "request.required.acks" "-1" ; all in-sync brokers
                    "producer.type"         "sync" 
                    "message.send.max_retries" "1" 
                    "connect.timeout.ms"    "1000"
                    "retry.backoff.ms"       "1000"
                    "serializer.class"     "kafka.serializer.DefaultEncoder"
                    "partitioner.class"    "kafka.producer.DefaultPartitioner"})]

    (reify SetApp
      (setup [app]
        (teardown app))

      (add [app element]
        (try
          (timeout 100000 error
                   (->> element
                        codec/encode
                        (producer/message "jepsen")
                        (producer/send-message producer))
                   ok)
          (catch org.apache.zookeeper.KeeperException$ConnectionLossException e
            error)))

      (results [app]
        (->> (drain! opts topic 1000)
             (map :value)
             (map codec/decode)))

      (teardown [app]
        (prn "Teardown: " (map (comp codec/decode :value)
                               (drain! opts topic 1000)))))))

(defn parse-int-list
  "Parses a string like 1,2,3,4,5 into a list of ints."
  [s]
  (->> (string/split s #",")
       (map #(Integer. %))))

(defn state
  "Returns a structure like:

  {topic-name {:partition n
               :leader n
               :replics [1 2 3 4 5]
               :isr     [1 2 3]}}}"
  [node]
  (control/on node
              (->> (control/exec "/opt/kafka/bin/kafka-list-topic.sh"
                                 :--zookeeper "localhost:2181")
                   string/split-lines
                   (map (fn [line]
                          ; Split line, take pairs, and convert to a map
                          (->> (string/split line #"\s+")
                               (partition 2)
                               (map (fn [[k v]]
                                      (let [k (-> k
                                                  (subs 0 (dec (count k)))
                                                  keyword)]
                                        [k (case k
                                             :partition (Integer. v)
                                             :leader    (Integer. v)
                                             :replicas  (parse-int-list v)
                                             :isr       (parse-int-list v)
                                                        v)])))
                               (into {}))))
                   (group-by :topic))))

(defn biggest-leader
  "Returns the string node name of the leader with the most jepsen partitions."
  [node]
  (-> node
      state
      (get "jepsen")
      (->> (group-by :leader)
           (sort-by (comp count val))
           last
           key
           (str "n"))))

(defn node->id
  "Turns n1 into 1"
  [node-name]
  (Integer. (nth (re-find #"(\d+)" node-name) 1)))

(def failure
  (let [leader (atom nil)]
    (reify failure/Failure

      (fail [_ nodes]
        ; Determine leader
        (reset! leader (biggest-leader (first nodes)))

        ; Isolate that leader in the Kafka protocol only
        (control/on @leader
                    (control/su
                      (->> nodes
                           (remove #{@leader})
                           (map control.net/ip)
                           (map #(control/exec :iptables
                                               :-A :INPUT
                                               :-p :tcp
                                               :-s %
                                               :--dport 9092
                                               :-j :DROP))
                           dorun)))
        (log @leader "kafka isolated"))

      (recover [_ nodes]
        ; Isolate the node altogether, causing it to lose its ZK conn
        (log "Totally partitioning" @leader)
        (control/on @leader
                    (control/su
                      (->> nodes
                           (remove #{@leader})
                           (map control.net/ip)
                           (map #(control/exec :iptables
                                               :-A :INPUT
                                               :-s %
                                               :-j :DROP))
                           dorun)))

        ; Block until leaders have transitioned.
        (loop []
          (let [parts (-> (remove #{@leader} nodes)
                          first
                          state
                          (get "jepsen")
                          (->> (group-by :leader))
                          (get (node->id @leader)))]
            (when-not (empty? parts)
              (log "Waiting for takeover of" parts)
              (sleep 1000)
              (recur))))

        (control/on-many nodes (control.net/heal))
        (log "Partition healed")))))
