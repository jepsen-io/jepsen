(ns jepsen.nemesis.file-test
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]
                     [test :refer :all]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [core :as jepsen]
                    [common-test :refer [quiet-logging]]
                    [control :as c]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.file :as nf]))

(use-fixtures :once quiet-logging)

(deftest ^:integration corrupt-file-chunks-helix-test
  ; This isn't going to work on containers, but I at least want to test that it
  ; uploads and compiles the binary.
  (let [file "/tmp/corrupt-demo"
        ; A file is a series of chunks made up of words.
        word-size   3 ; the word "32 " denotes word 2 in chunk 3.
        chunk-size  (* word-size 4)
        chunk-count 5
        test (assoc tests/noop-test
                    :name      "corrupt-file-test"
                    :nemesis   (nf/corrupt-file-nemesis)
                    :generator
                    (->> (nf/corrupt-file-chunks-helix-gen
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
