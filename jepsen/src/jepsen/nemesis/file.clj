(ns jepsen.nemesis.file
  "Fault injection involving files on disk."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [util :as util]]))

(defn corrupt-file!
  "Calls corrupt-file on the currently bound remote node, taking a map of
  options."
  [{:keys [mode file start end chunk-size mod i]}]
  (assert (string? file))
  (c/su
    (c/exec (str nemesis/bin-dir "/corrupt-file")
            (when mode        [:--mode mode])
            (when start       [:--start start])
            (when end         [:--end end])
            (when chunk-size  [:--chunk-size chunk-size])
            (when mod         [:--modulus mod])
            (when i           [:--index i])
            file)))

(defn clear-snapshots!
  "Asks corrupt-file! to clear its snapshots directory on the currently bound
  remote node."
  []
  (c/su
    (c/exec (str nemesis/bin-dir "/corrupt-file") :--clear-snapshots)))

(defn f->mode
  "Turns an op :f into a mode for the corrupt-file binary."
  [f]
  (case f
    :copy-file-chunks       "copy"
    :snapshot-file-chunks   "snapshot"
    :restore-file-chunks    "restore"))

(defrecord CorruptFileNemesis
  ; A map of default options, e.g. {:chunk-size ..., :file, ...}, which are
  ; merged as defaults into each corruption we perform.
  [default-opts]

  nemesis/Reflection
  (fs [this]
    #{:corrupt-file-chunks})

  nemesis/Nemesis
  (setup! [this test]
    (c/with-test-nodes test
      (nemesis/compile-c-resource! "corrupt-file.c" "corrupt-file")
      (clear-snapshots!))
    this)

  (invoke! [this test {:keys [f value] :as op}]
    (assert (every? string? (map :node value)))
    (case f
      (:copy-file-chunks
       :snapshot-file-chunks
       :restore-file-chunks)
      (let [v (c/on-nodes
                test (distinct (map :node value))
                (fn node [test node]
                  ; Apply each relevant corruption
                  (->> value
                       (keep (fn corruption [corruption]
                               (let [corruption
                                     (-> default-opts
                                         (assoc :mode (f->mode f))
                                         (merge corruption))]
                                 (when (= node (:node corruption))
                                   (corrupt-file! corruption)))))
                       (into []))))]
        (assoc op :value v))))

  (teardown! [this test]))

(defn corrupt-file-nemesis
  "This nemesis takes operations like

    {:f     :copy-file-chunks
     :value [{:node       \"n2\"
              :file       \"/foo/bar\"
              :start      128
              :end        256
              :chunk-size 16
              :mod        5
              :i          2}}
             ...]}

  This corrupts the file /foo/bar on n2 in the region [128, 256) bytes,
  dividing that region into 16 KB chunks, then corrupting every fifth chunk,
  starting with (zero-indexed) chunk 2: 2, 7, 12, 17, .... Data is copied from
  other chunks in the region which are *not* interfered with by this command,
  unless mod is 1, in which case chunks are randomly selected. The idea is that
  this gives us a chance to produce valid-looking structures which might be
  dereferenced by later pointers.

    {:f     :snapshot-file-chunks
     :value [{:node       \"n2\"
              :file       \"/foo/bar\"
              :start      128
              :end        256
              :chunk-size 16
              :mod        5
              :i          2}}
             ...]}

  :snapshot-file-chunks uses the same start/end/chunk logic as corrupt-file,
  chunks. However, instead of corrupting chunks, it copies them to files in
  /tmp. These chunks can be restored by a corresponding :restore-file-chunks
  operation:

    {:f     :restore-file-chunks
     :value [{:node       \"n2\"
              :file       \"/foo/bar\"
              :start      128
              :end        256
              :chunk-size 16
              :mod        5
              :i          2}}
             ...]}

  This uses the same start/end/chunk logic, but copies data from the most
  recently snapshotted chunks back into the file itself. You can use this to
  force what looks like a rollback of parts of a file's state--for instance, to
  simulate a misdirected or lost write.

  All options are optional, except for :node and :file. See
  resources/corrupt-file.c for defaults.

  This function can take an optional map with defaults for each file-corruption
  operation."
  ([]
   (corrupt-file-nemesis {}))
  ([default-opts]
   (CorruptFileNemesis. default-opts)))

(defrecord HelixGen
  [; Produces :f :corrupt-file-chunks (or similar), and :value nil.
   f-gen
   ; Map of default options for each corruption.
   default-opts]
  gen/Generator
  (op [this test ctx]
    ; We immediately unfurl into a series of ops based on f-gen with values
    ; derived from our node choices.
    (let [nodes (shuffle (:nodes test))
          value (mapv (fn per-node [i node]
                        (assoc default-opts
                               :node node
                               :mod  (count nodes)
                               :i    i))
                      (range)
                      nodes)]
      (gen/op (gen/map (fn per-op [op] (assoc op :value value))
                       f-gen)
              test ctx)))

  (update [this test ctx op]
    this))

(defn copy-file-chunks-helix-gen
  "Generates copy-file-chunks operations in a 'helix' around the cluster.
  Once per test, picks random `i`s for the nodes in the test. Takes default
  options for file corruptions, as per `corrupt-file-nemesis`. Emits a series
  of `copy-file-chunks` operations, where each operation has a file
  corruption on every node. Because the `i` for each node is fixed, this
  ensures that no two nodes ever corrupt the same bit of the file. If the
  permutation of node is is [n1, n2, n3], and with a chunk size of 2 bytes,
  this looks like:

    node   file bytes
           0123456789abcde ...
    n1     ╳╳    ╳╳    ╳╳
    n2       ╳╳    ╳╳    ╳
    n3         ╳╳    ╳╳

  This seems exceedingly likely to destroy a cluster, but some systems may
  survive it. In particular, systems which keep their on-disk representation
  very close across different nodes may be able to recover from the intact
  copies on other nodes."
  ([]
   (copy-file-chunks-helix-gen {}))
  ([default-opts]
   (HelixGen. (gen/repeat {:type :info, :f :copy-file-chunks})
              default-opts)))

(defrecord NodesGen [n f-gen default-opts]
  gen/Generator
  (op [this test ctx]
    ; Pick nodes we're allowed to interfere with
    (let [n (if (number? n)
              n
              (n test))
          _ (assert (integer? n)
                    (str "Expected (n test) to return an integer, but got "
                         (pr-str n)))
          nodes (->> (:nodes test) shuffle (take n) vec)]
      ; Unfurl into f-gen, with values rewritten to random subsets of those
      ; nodes.
      (gen/op
        (gen/map (fn op [op]
                   (let [value (mapv (fn per-node [node]
                                       (assoc default-opts
                                              :node node))
                                     (util/random-nonempty-subset nodes))]
                     (assoc op :value value)))
                 f-gen)
        test ctx))))

(defn snapshot-file-chunks-nodes-gen
  "Generates alternating :snapshot-file-chunks and :restore-file-chunks
  operations on up to `n` nodes in the cluster. Selects up to n nodes when
  first invoked, and always chooses random non-empty subsets of those nodes.
  Takes default options for file corruptions, as per `corrupt-file-nemesis.`

  `n` can also be a function `(n test)`, which can be used to select (e.g.) a
  minority of nodes. For example, try `(comp jepsen.util/minority count
  :nodes)`.

  This generator is intended to stress systems which can tolerate up to n disk
  faults, but no more."
  ([n]
   (snapshot-file-chunks-nodes-gen n {}))
  ([n default-opts]
   (NodesGen.
     n
     (gen/flip-flop
       (gen/repeat {:type :info, :f :snapshot-file-chunks})
       (gen/repeat {:type :info, :f :restore-file-chunks}))
     default-opts)))
