(ns jepsen.nemesis-test
  (:require [clojure [pprint :refer [pprint]]
                     [set :as set]
                     [test :refer :all]]
            [jepsen [client :as client]
                    [common-test :refer [quiet-logging]]
                    [control :as c]
                    [core :as jepsen]
                    [generator :as gen]
                    [nemesis :refer :all]
                    [tests :as tests]
                    [util :refer [meh]]]
            [jepsen.control.net :as net]))

(defn edges
  "A map of nodes to the set of nodes they can ping"
  [test]
  (c/on-many (:nodes test)
             (into (sorted-set) (filter net/reachable? (:nodes test)))))

(deftest bisect-test
  (is (= (bisect []) [[] []]))
  (is (= (bisect [1]) [[] [1]]))
  (is (= (bisect [1 2 3 4]) [[1 2] [3 4]]))
  (is (= (bisect [1 2 3 4 5]) [[1 2] [3 4 5]])))

(deftest complete-grudge-test
  (is (= (complete-grudge (bisect [1 2 3 4 5]))
         {1 #{3 4 5}
          2 #{3 4 5}
          3 #{1 2}
          4 #{1 2}
          5 #{1 2}})))

(deftest bridge-test
  (is (= (bridge [1 2 3 4 5])
         {1 #{4 5}
          2 #{4 5}
          4 #{1 2}
          5 #{1 2}})))

(defn symmetric-grudge?
  "Is the given grudge map symmetric?"
  [grudge]
  (every? (fn [[k vs]]
            (every? (fn [v] (contains? (get grudge v) k)) vs))
          grudge))

(deftest majorities-ring-test
  (let [nodes  (range 5)
        grudge (majorities-ring nodes)]
    (is (= (count grudge) (count nodes)))
    (is (= (set nodes) (set (keys grudge))))
    (is (every? (partial = 2) (map count (vals grudge))))
    (is (every? (fn [[node snubbed]]
                  (not-any? #{node} snubbed))
                grudge))
    (is (distinct? (vals grudge))))

  (testing "five-node-ring"
    ; With exactly five nodes, we should obtain the degenerate case where every
    ; node can talk to its two nearest neighbors symmetrically. This means we
    ; should be able to traverse the ring in one direction, then go back the
    ; other way.
    (let [nodes   (range 5)
          grudge  (majorities-ring nodes)
          U       (set (keys grudge))
          start   (key (first grudge))
          path    (loop [from    nil
                         node    start
                         return? false
                         path    []]
                    (let [vis (set/difference U (get grudge node))]
                      ; Should be exactly 3 connections
                      (is (= 3 (count vis)))
                      ; Nodes should see themselves
                      (is (contains? vis node))
                      ; Move on
                      (if (and from (= start node))
                        (if return?
                          (conj path node)                         ; we're done
                          (recur node from true (conj path node))) ; reverse
                        ; Move on
                        (let [node' (-> vis
                                        (disj node)
                                        (disj from)
                                        first)]
                          (recur node
                                 node'
                                 return?
                                 (conj path node))))))]
      (testing "path covers all nodes"
        (is (= U (set path))))
      (testing "path is palindromic"
        (is (= path (reverse path))))
      (testing "path is properly sized"
        (is (= (inc (* 2 (count U))) (count path))))))

  (testing "10 nodes"
    (let [nodes  (range 10)
          grudge (majorities-ring nodes)
          U      (set (keys grudge))]
      ; If a -> b, b -> a
      (is (symmetric-grudge? grudge))
      ; Every node has a grudge
      (is (= (set nodes) U))
      ; Every node can see a majority
      (is (every? #(< (count %) 5) (vals grudge)))
      ; But every node bans at least 3 nodes.
      (is (every? #(<= 3 (count %)) (vals grudge))))))

(deftest simple-partition-test)
  ;(let [n (partition-halves)]
  ;  (try
  ;    (client/setup! n noop-test nil)
  ;    (is (= (edges noop-test)
  ;           {:n1 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n2 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n3 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n4 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n5 #{:n1 :n2 :n3 :n4 :n5}}))

  ;    (client/invoke! n noop-test {:f :start})
  ;    (is (= (edges noop-test)
  ;           {:n1 #{:n1 :n2}
  ;             :n2 #{:n1 :n2}
  ;             :n3 #{:n3 :n4 :n5}
  ;             :n4 #{:n3 :n4 :n5}
  ;             :n5 #{:n3 :n4 :n5}}))

  ;    (client/invoke! n noop-test {:f :stop})
  ;    (is (= (edges noop-test)
  ;           {:n1 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n2 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n3 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n4 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n5 #{:n1 :n2 :n3 :n4 :n5}}))

  ;    (finally
  ;      (client/teardown! n noop-test)))))

(defrecord TestNem [id fs setup? teardown?]
  Nemesis
  (setup! [this test]
    (assoc this :setup? true))

  (invoke! [this test op]
    (assert (contains? fs (:f op)))
    (assoc op :value this))

  (teardown! [this test]
    (reset! teardown? true))

  Reflection
  (fs [this]
    fs))

(defn test-nem
  "Constructs a test nemesis. Takes an ID (anything) and a set of fs."
  [id fs]
  (TestNem. id fs false (atom false)))

(deftest compose-test
  (testing "reflection"
    (let [a (test-nem :a #{:a})
          b (test-nem :b #{:b})
          c (compose [a b])]
      (is (= #{:a} (fs a)))
      (is (= #{:b} (fs b)))
      ; Composed nemesis should handle both fs
      (is (= #{:a :b} (fs c)))
      (is (every? (comp false? :setup?) (:nemeses c)))
      (is (every? (comp false? deref :teardown?) (:nemeses c)))
      (let [c' (setup! c {})]
        ; Should propagate setup! through to children
        (is (= #{:a :b} (fs c')))
        (is (every? (comp true? :setup?) (:nemeses c')))
        (is (every? (comp false? deref :teardown?) (:nemeses c')))

        ; Should evaluate on sub-nemeses
        (let [[a' b'] (:nemeses c')]
          (is (= :a (:id a')))
          (is (= :b (:id b')))
          (is (= a' (:value (invoke! c' {} {:type :info, :f :a}))))
          (is (= b' (:value (invoke! c' {} {:type :info, :f :b})))))

        ; Should propagate teardown! through to children
        (teardown! c' {})
        (is (= #{:a :b} (fs c')))
        (is (every? (comp true? :setup?) (:nemeses c')))
        (is (every? (comp true? deref :teardown?) (:nemeses c')))))))

(defn nonzero-file?
  "Are there non-zero bytes in a file?"
  [file]
  ; This is kind of a weird hack but we're just going to grep for something
  ; other than all-zeroes in the hexdump.
  (let [data' (c/on "n1"
                    (c/su
                      (c/exec :hd file c/| :head :-n 10)))]
    (boolean (re-find #"\s([1-9]0|0[1-9])\s" data'))))

(deftest ^:integration bitflip-test
  ; First, create a file on n1 with a bunch of zeroes.
  (let [file "/tmp/jepsen/zeroes"]
    (c/on "n1"
          (c/su
            (c/exec :mkdir :-p "/tmp/jepsen")
            (c/exec :rm :-rf file)
            (c/exec :dd "if=/dev/zero" (str "of=" file) "bs=1M" "count=1")))
    (is (not (nonzero-file? file)))
    ; Then run a test with a nemesis that flips some bits.
    (let [test (assoc tests/noop-test
                      :name      "bitflip test"
                      :nemesis   (bitflip)
                      :generator (gen/nemesis
                                   {:type :info
                                    :f    :bitflip
                                    :value {"n1" {:file file
                                                  :probability 0.01}}}))
          test' (jepsen/run! test)]
      ; We should see those bitflips!
      (is (nonzero-file? file)))))
