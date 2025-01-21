(ns jepsen.nemesis.file-test
  (:require [clojure [pprint :refer [pprint]]
                     [set :as set]
                     [string :as str]
                     [test :refer :all]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [client :as client]
                    [core :as jepsen]
                    [common-test :refer [quiet-logging]]
                    [control :as c]
                    [generator :as gen]
                    [history :as h]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.file :as nf]))

(use-fixtures :once quiet-logging)

(deftest ^:integration copy-file-chunks-helix-test
  (let [file "/tmp/corrupt-demo"
        ; A file is a series of chunks made up of words.
        word-size   3 ; the word "32 " denotes word 2 in chunk 3.
        chunk-size  (* word-size 4)
        chunk-count 5
        test (assoc tests/noop-test
                    :name      "corrupt-file-test"
                    :nemesis   (nf/corrupt-file-nemesis)
                    :generator
                    (->> (nf/copy-file-chunks-helix-gen
                           {:file       file
                            :chunk-size chunk-size})
                         ; We do three passes to make sure it leaves
                         ; untouched chunks correct
                         (gen/limit 3)
                         gen/nemesis))
        ; Start by creating files with predictable bytes
        string (str/join (for [chunk (range chunk-count)
                              i     (range (/ chunk-size word-size))]
                          (str chunk i " ")))
        nodes (:nodes test)
        n (count nodes)
        _ (c/on-many nodes
            (cu/write-file! string file))

        ; To parse these strings back into vectors of chunks...
        parse-word (fn [[chunk word space]]
                     (assert (= \space space))
                     [(parse-long (str chunk))
                      (parse-long (str word))])
        parse-chunk (fn [chunk]
                      (mapv parse-word (partition-all word-size chunk)))
        parse (fn [data]
                (mapv parse-chunk (partition-all chunk-size data)))
        data (parse string)

        ; Valid chunks
        valid-chunk? (set data)

        ; Run test
        test' (jepsen/run! test)
        h (:history test')
        ]
    (is (= 6 (count h)))
    ; (pprint h)
    ; Check contents of files
    (c/on-many nodes
      (let [string' (c/exec :cat file)
            data' (parse string')]
        ; (info data')
        ; Obviously
        (is (not= data data'))
        ; Same length
        (is (= (count string) (count string')))
        ; But every chunk should be valid
        (is (every? valid-chunk? data))
        ; And the modulos of the bad chunks are all the same
        (->> data'
             ; Extract expected and actual chunk IDs (these will all be the
             ; same because chunks are valid)
             (map-indexed (fn [expected-i chunk]
                            [expected-i (first (first chunk))]))
             ; Retain just the modulos of those which differ
             (keep (fn [[expected actual]]
                       (when (not= expected actual)
                         (mod actual n))))
             set
             count
             (= 1) is)))))

(deftest ^:focus ^:integration snapshot-file-chunks-helix-test
  (let [file "/tmp/snapshot-demo"
        start      1
        end        5
        test (assoc tests/noop-test
                    :nodes     (take 3 (:nodes tests/noop-test))
                    :name      "snapshot-file-test"
                    :nemesis   (nf/corrupt-file-nemesis)
                    ; We want to change the contents of the file between
                    ; nemesis ops
                    :client (reify client/Client
                              (open! [this test node] this)
                              (setup! [this test]
                                (c/with-test-nodes test
                                  (c/su
                                    (cu/write-file! "aa" file))))
                              (invoke! [this test op]
                                (let [res (c/with-test-nodes test
                                            (c/su
                                              (let [before (c/exec :cat file)
                                                    _ (cu/write-file! (:value op) file)
                                                    after (c/exec :cat file)]
                                                [before after])))]
                                  (assoc op :type :ok, :value res)))
                              (teardown! [this test])
                              (close! [this test]))
                    :generator
                    (->> (gen/flip-flop
                           (gen/nemesis
                             (nf/snapshot-file-chunks-nodes-gen
                               (comp dec util/majority count :nodes)
                               {:file       file
                                :start      start
                                :end        end}))
                           (gen/clients
                             [{:f :w, :value "bbbb"}
                              {:f :w, :value "ccccccccccc"}
                              {:f :w, :value "dddddddddd"}
                              {:f :w, :value "e"}]))
                         (gen/limit 8)
                         (gen/concurrency-limit 1)))
        ; Run test
        test' (jepsen/run! test)
        h (:history test')
        ;_ (pprint h)
        ; Extract the nodes we acted on
        affected-nodes (->> (filter (comp #{:nemesis} :process) h)
                            (map :value)
                            (filter map?)
                            (map (comp set keys))
                            (reduce set/union))]
    ; Should be just one node
    (is (= 1 (count affected-nodes)))

    ; On affected nodes...
    (is (= [; We start with a, snapshot, and write out four bs
            ["aa" "bbbb"]
            ; Nemesis restores [ a], we write c
            ["babb" "ccccccccccc"]
            ; Nemesis snapshots c, we write d
            ["ccccccccccc" "dddddddddd"]
            ; Nemesis restores c, we write e
            ["dccccddddd" "e"]
            ]
           (->> (h/client-ops h)
                (h/filter h/ok?)
                (h/map (fn [op]
                         (get (:value op)
                              (first affected-nodes)))))))))
