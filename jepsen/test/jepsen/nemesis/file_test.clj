(ns jepsen.nemesis.file-test
  (:require [clj-commons.byte-streams :as bs]
            [clojure [pprint :refer [pprint]]
                     [set :as set]
                     [string :as str]
                     [test :refer :all]]
            [clojure.java [io :as io]
                          [shell :as shell]]
            [clojure.test.check [clojure-test :refer [defspec]]
                                [generators :as g]
                                [properties :as prop]
                                [results :as results :refer [Result]]
                                [rose-tree :as rose]]
            [clojure.tools.logging :refer [info warn]]
            [com.gfredericks.test.chuck.clojure-test
             :as chuck
             :refer [checking for-all]]
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
            [jepsen.nemesis.file :as nf]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.nio.channels FileChannel
                              FileChannel$MapMode)
           (java.nio.file Files
                          Path
                          Paths
                          StandardOpenOption)))

(def test-n
  "Number of trials for generative tests"
  100)

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
                    (->> {:type :info, :f :copy-file-chunks}
                         gen/repeat
                         (nf/helix-gen
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

(deftest ^:integration snapshot-file-chunks-helix-test
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
                           (->> (gen/flip-flop (gen/repeat {:type :info, :f :snapshot-file-chunks})
                                               (gen/repeat {:type :info, :f :restore-file-chunks}))
                                (nf/nodes-gen (comp dec util/majority count :nodes)
                                              {:file       file
                                               :start      start
                                               :end        end})
                                gen/nemesis)
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

(deftest ^:integration bitflip-file-chunks-helix-test
  (let [file "/tmp/bitflip-demo"
        start      4
        end        11
        chunk-size 3
        index      2
        probability 0.5
        test (assoc tests/noop-test
                    :nodes     (take 3 (:nodes tests/noop-test))
                    :name      "bitflip-file-test"
                    :nemesis   (nf/corrupt-file-nemesis)
                    :generator
                    (->> {:type :info, :f :bitflip-file-chunks}
                         (nf/nodes-gen
                           (comp util/minority count :nodes)
                           {:file       file
                            :start      start
                            :end        end
                            :chunk-size chunk-size
                            :index      index
                            :probability probability})
                         gen/nemesis
                         (gen/limit 1)))
        ; Run test
        data "ffffffffffffffffffffffff"
        _ (c/on-many (:nodes test)
            (c/su (cu/write-file! data file)))
        ; Read back as hex
        data (c/on (first (:nodes test))
                   (c/su (c/exec :xxd :-p file)))
        test' (jepsen/run! test)
        h (:history test')
        ;_ (pprint h)
        ; Extract the nodes we acted on
        affected-nodes (->> (filter (comp #{:nemesis} :process) h)
                            (map :value)
                            (filter map?)
                            (map (comp set keys))
                            (reduce set/union))
        ; Should be just one node
        _ (is (= 1 (count affected-nodes)))
        node (first affected-nodes)
        ; What's the file like there?
        data' (c/on node
                   (c/su (c/exec :xxd :-p file)))]
    ; Should be unaffected outside range
    (is (= (subs data 0 (* 2 start)) (subs data' 0 (* 2 start))))
    (is (= (subs data (* 2 end)) (subs data' (* 2 end))))
    ; But modified in the range!
    (is (not= data data'))))

;; Local generative tests

(def src
  "Source file."
  "resources/corrupt-file.c")

(def dir
  "Temp dir for binaries, data files, etc."
  "/tmp/jepsen/nemesis/file")

(def data-file
  "Our original data file."
  (str dir "/data"))

(def data-file'
  "Our corrupted data file."
  (str dir "/data2"))

(def data-file''
  "Sometimes you need three steps, you know?"
  (str dir "/data3"))

(def bin
  "Binary file"
  (str dir "/corrupt-file"))

(defn sh
  "Runs shell command, throws on nonzero exit"
  [& args]
  (let [res (apply shell/sh args)]
    (when (not= 0 (:exit res))
      (throw+ (assoc res
                     :cmd args)))
    res))

(defn build-local!
  "Fixture for tests. Compiles the corrupt-file binary locally, stashing it in
  /tmp/jepsen/bin/corrupt-file. Wipes the directory when done."
  [f]
  (sh "mkdir" "-p" dir)
  (sh "gcc" src "-o" bin "-lm" "-Wall" "-Wextra")
  (f)
  #_(sh "rm" "-rf" dir))

(use-fixtures :once build-local!)

(def smol-int
  "Very small positive integers, like mod and index."
  (g/large-integer* {:min 0
                     :max 3}))

(def medium-int
  "Medium-sized positive integers. 1 K at most keeps our tests reasonably
  fast."
  (g/large-integer* {:min 0
                     :max (* 1024)}))

(def large-int
  "Large integer generator for file sizes"
  (g/large-integer* {:min 0
                     ; We explicitly want to test over the 4 GB limit
                     :max (* 8 1024 1024 1024)}))

(def chunk-opts
  "Generator for chunk options."
  (g/let [i          smol-int
          mod        smol-int
          chunk-size g/nat
          start      medium-int
          end        medium-int
          size       medium-int]
    {:index       i
     :modulus     (+ i (inc mod))
     :chunk-size  (inc chunk-size)
     :start       start
     :end         (+ start end)
     ; We provide a file size here too
     :size        size}))

(defn prob-gen
  "Probability generator. Double between 0 and 1. Takes chunk options."
  [{:keys [chunk-size start end size]}]
  (g/let [scale (g/double* {:min 0.5, :max 1.5})]
    ; For small files, we want a high probability, otherwise we won't change
    ; anything. For large files, smaller p is fine; we want a few flips per
    ; chunk though.
    (let [bytes (-> (min size end)    ; Last byte in file we affect
                    (- start)         ; Bytes in affected region
                    (min chunk-size)  ; Or bytes in chunk, whichever is smaller
                    (max 0))]
      (if (= 0 bytes)
        ; Doesn't matter
        nil
        (-> bytes
            (* 8)          ; Bytes->bits
            /              ; Mean probability of one per chunk
            (* 3)          ; To be safe
            (* scale)      ; Noise
            (min 1.0)))))) ; Probability

(defn corrupt!
  "Calls corrupt-file with options from the given map."
  [opts]
  (apply sh bin
         (map str
              (concat
                (when-let [c (:chunk-size opts)]  ["--chunk-size" c])
                (when-let [m (:mode opts)]        ["--mode" m])
                (when-let [i (:index opts)]       ["--index" i])
                (when-let [m (:modulus opts)]     ["--modulus" m])
                (when-let [s (:start opts)]       ["--start" s])
                (when-let [e (:end opts)]         ["--end" e])
                (when-let [p (:probability opts)] ["--probability" p])
                [data-file']))))

(defn make-file!
  "Creates a data file of size, drawn from device dev. Returns file."
  ([dev size]
   (make-file! dev size data-file))
  ([dev size data-file]
   (sh "rm" "-f" data-file)
   (let [chunk-size (* 1024 1024)
         remainder (rem size chunk-size)
         chunks (long (/ (- size remainder) chunk-size))]
     (assert (= size (+ (* chunks chunk-size) remainder)))
     (sh "touch" data-file)
     (when (pos? chunks)
       ; Fill with chunks (for large files)
       (sh "dd"
           (str "if=" dev)
           (str "of=" data-file)
           (str "bs=" chunk-size)
           (str "count=" chunks)))
     (when (pos? remainder)
       ; Then leftovers
       (sh "dd"
           "conv=notrunc"
           "oflag=append"
           (str "if=" dev)
           (str "of=" data-file)
           (str "bs=" remainder)
           "count=1"))
     ; Double-check
     (assert (= size
                (-> data-file
                    io/file
                    .length))))
   data-file))

(defn make-files!
  "Like make-file!, but also creates an identical data-file'."
  [dev size]
  (let [f (make-file! dev size)]
    (sh "cp" f data-file'))
  data-file')

(defn corrupted-chunks
  "Which chunk indices are affected by the given options? Infinite seq."
  [{:keys [index modulus]}]
  (->> (range)
       (filter (fn [i]
                 (= index (mod i modulus))))))

(defn intact-chunks
  "Which chunk indices are unaffected by the given options? Infinite seq."
  [{:keys [index modulus]}]
  (if (< modulus 2)
    nil
    (->> (range)
         (remove (fn [i]
                   (= index (mod i modulus)))))))

(defn open
  "Opens a filechannel to the given path, as a string"
  [file]
  (-> file
      io/file
      .toPath
      (FileChannel/open
        (into-array [StandardOpenOption/READ]))))

(defn buffers
  "Takes a FileChannel and an options map. Returns a map of:

    {:corrupted (ByteBuffer...)
     :intact    (ByteBuffer...)

  For all the regions of the file which we think should have been corrupted, or
  left intact."
  [^FileChannel file, {:keys [start end chunk-size] :as opts}]
  (let [ro FileChannel$MapMode/READ_ONLY
        file-size (.size file)
        start     (or start 0)
        end       (or end file-size)
        ; Turn an upper bound, a maximum buffer size, and an offset into a
        ; ByteBuf into the file, or nil if out of bounds.
        buffer (fn buffer [upper-bound max-size start]
                 (let [end (min (+ start max-size)
                                file-size
                                upper-bound)
                       size (- end start)]
                   (when (< 0 size)
                     (.map file ro start size))))
        ; The region before/after the chunks might be bigger than we can map,
        ; so we break it up into 16M buffers
        buf-size  (* 16 1024 1024)
        ; The intact region before the start
        prefix (->> (range 0 start buf-size)
                    (keep (partial buffer start buf-size)))
        ; And at the end
        postfix (->> (range end file-size buf-size)
                    (keep (partial buffer file-size buf-size))
                    (take-while identity))
        ; Turn a chunk number into a buffer, or nil if out of bounds.
        chunk (fn chunk [i]
                (let [chunk-start (+ start (* i chunk-size))]
                  (buffer end chunk-size chunk-start)))
        ; Corrupted chunks
        corrupted (->> (corrupted-chunks opts)
                       (map chunk)
                       (take-while identity))
        intact    (->> (intact-chunks opts)
                       (map chunk)
                       (take-while identity))
        intact    (concat prefix
                          intact
                          postfix)]
    {:corrupted corrupted
     :intact    intact}))

(defn equal-p
  "Given two sequences of bytebuffers, what is the probability two
  corresponding buffers are equal? We do this probabilistically because
  sometimes a copy or bitflip or whatever will actually leave data intact."
  [as bs]
  (if (and (nil? (seq as))
           (nil? (seq bs)))
    ; Trivial case: no chunks in either
    1
    (loop [count  0
           equal  0
           as     as
           bs     bs]
      (let [[a & as'] as
            [b & bs'] bs]
        ;(prn :compare a b)
        (cond ; Clean exit
              (and (nil? a) (nil? b))
              (/ equal count)

              ; Out of as
              (nil? a) (recur (inc count) equal as' bs')

              ; Out of bs
              (nil? b) (recur (inc count) equal as' bs')

              true
              (recur (inc count)
                     (if (bs/bytes= a b)
                       (inc equal)
                       equal)
                     as'
                     bs'))))))

(defn not-equal-p
  "Given two sequences of bytebuffers, what is the probability the two are not
  equal?"
  [as bs]
  (- 1 (equal-p as bs)))

(defn bitflip-test-
  "Helper for bitflip tests."
  [opts]
  (println "------")
  (pprint opts)

  (make-files! "/dev/zero" (:size opts))
  (let [res (corrupt! (assoc opts
                             :mode "bitflip"))]
    (print (:out res)))

  ; Check that file sizes are equal
  (is (= (-> data-file  io/file .length)
         (-> data-file' io/file .length)))

  ; Check chunks
  (with-open [data  (open data-file)
              data' (open data-file')]
    (let [{:keys [corrupted intact]}  (buffers data  opts)
          buffers' (buffers data' opts)
          corrupted' (:corrupted buffers')
          intact'    (:intact buffers')]
      (is (= 1 (equal-p intact intact')))
      (when (seq corrupted')
        ; This is fairly weak--with higher p and chunks we should be
        ; able to do better.
        (is (< (equal-p corrupted corrupted') 1))))))

(deftest bitflip-test
  (checking "basic" test-n
            [opts chunk-opts
             ; Our prob-gen isn't smart enough to figure out when the mod and
             ; index would end up just picking, say, one byte out of the
             ; file--it can generate far too small probabilties. We hardcode
             ; this to 1/8.
             ;p    (prob-gen opts)
             ]
            (bitflip-test- (assoc opts :probability 0.125))))

(deftest ^:slow bitflip-large-test
  ; This stresses our large file dynamics. 8 GB file, trimmed 1 GB from both
  ; ends.
  (let [size (* 1024 1024 1024 8)]
    (bitflip-test- {:modulus 128
                    :index 1
                    :chunk-size (* 1024 1024 16)
                    :start  (* 1 1024 1024 1024)
                    :end    (- size (* 1 1024 1024 1024))
                    :size   size
                    :probability (double (/ 1 1024 1024 8))})))

(defn copy-test-
  "Helper for copy tests."
  [opts]
  (println "------")
  (pprint opts)

  (make-files! "/dev/random" (:size opts))
  (let [res (corrupt! (assoc opts
                             :mode "copy"))]
    (print (:out res)))

  ; Check that file sizes are equal
  (is (= (-> data-file  io/file .length)
         (-> data-file' io/file .length)))

  ; Check chunks
  (with-open [data  (open data-file)
              data' (open data-file')]
    (let [{:keys [corrupted intact]}  (buffers data opts)
          buffers' (buffers data' opts)
          corrupted' (:corrupted buffers')
          intact'    (:intact buffers')]
      (is (= 1 (equal-p intact intact')))
      (when (and (seq corrupted')
                 ; We need somewhere else to copy from
                 (seq intact'))
        (is (< (equal-p corrupted corrupted') 1/4))))))

(deftest copy-test
  (checking "basic" test-n
            [opts chunk-opts]
            (copy-test- opts)))

(deftest ^:slow copy-slow-test
  ; This stresses large file dynamics. 8GB file, trimmed 1 GB from both ends.
  (let [size (* 1024 1024 1024 8)]
    (copy-test- {:modulus 128
                 :index 1
                 :chunk-size (* 1024 1024 16)
                 :start  (* 1 1024 1024 1024)
                 :end    (- size (* 1 1024 1024 1024))
                 :size   size})))

(defn snapshot-test-
  "Helper for snapshot tests."
  [opts]
  (println "------")
  (pprint opts)

  (make-files! "/dev/random" (:size opts))
  (sh bin "--clear-snapshots")
  ; Take snapshots of original file
  (let [res (corrupt! (assoc opts :mode "snapshot"))]
    (print (:out res)))
  ; Now overwrite the fresh file with zeroes
  (make-file! "/dev/zero" (:size opts) data-file')
  ; And restore into it
  (let [res (corrupt! (assoc opts :mode "restore"))]
    (print (:out res)))

  ; Check that file sizes are equal
  (is (= (-> data-file  io/file .length)
         (-> data-file' io/file .length)))

  ; Check chunks
  (with-open [data  (open data-file)
              zero  (open "/dev/zero") ; Can you mmap /dev/zero? We'll find out
              data' (open data-file')]
    (let [buffers   (buffers data opts)
          buffers'  (buffers data' opts)
          zero      (buffers zero opts)]
      ; The "intact" regions should be all zeroes
      (is (= 1 (equal-p (:intact zero) (:intact buffers'))))
      ; And if we corrupted something, it should be identical to the
      ; original data
      (when (seq (:corrupted buffers'))
        (is (= 1 (equal-p (:corrupted buffers)
                          (:corrupted buffers'))))))))

(deftest ^:focus snapshot-test
  (checking "basic" test-n
            [opts chunk-opts]
            (snapshot-test- opts)))

(deftest ^:slow snapshot-slow-test
  ; This stresses large file dynamics. 8GB file, trimmed 1 GB from both ends.
  (let [size (* 1024 1024 1024 8)]
    (snapshot-test- {:modulus 128
                     :index 1
                     :chunk-size (* 1024 1024 16)
                     :start  (* 1 1024 1024 1024)
                     :end    (- size (* 1 1024 1024 1024))
                     :size   size})))
