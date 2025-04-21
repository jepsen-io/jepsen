(ns jepsen.nemesis.file
  "Fault injection involving files on disk. This nemesis can copy chunks
  randomly within a file, induce bitflips, or snapshot and restore chunks."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [util :as util]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn corrupt-file!
  "Calls corrupt-file on the currently bound remote node, taking a map of
  options."
  [{:keys [mode file start end chunk-size mod i probability]}]
  (assert (string? file))
  (c/su
    (c/exec (str nemesis/bin-dir "/corrupt-file")
            (when mode        [:--mode mode])
            (when start       [:--start start])
            (when end         [:--end end])
            (when chunk-size  [:--chunk-size chunk-size])
            (when mod         [:--modulus mod])
            (when i           [:--index i])
            (when probability [:--probability (double probability)])
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
    :bitflip-file-chunks    "bitflip"
    :copy-file-chunks       "copy"
    :restore-file-chunks    "restore"
    :snapshot-file-chunks   "snapshot"))

(defrecord CorruptFileNemesis
  ; A map of default options, e.g. {:chunk-size ..., :file, ...}, which are
  ; merged as defaults into each corruption we perform.
  [default-opts]

  nemesis/Reflection
  (fs [this]
    #{:bitflip-file-chunks
      :copy-file-chunks
      :restore-file-chunks
      :snapshot-file-chunks})

  nemesis/Nemesis
  (setup! [this test]
    (c/with-test-nodes test
      (nemesis/compile-c-resource! "corrupt-file.c" "corrupt-file")
      (clear-snapshots!))
    this)

  (invoke! [this test {:keys [f value] :as op}]
    (assert (every? string? (map :node value)))
    (case f
      (:bitflip-file-chunks
       :copy-file-chunks
       :restore-file-chunks
       :snapshot-file-chunks)
      (let [v (c/on-nodes
                test
                (distinct (map :node value))
                (fn node [test node]
                  ; Apply each relevant corruption
                  (->> value
                       (keep (fn corrupt! [corruption]
                               (let [corruption
                                     (-> default-opts
                                         (assoc :mode (f->mode f))
                                         (merge corruption))]
                                 (when (= node (:node corruption))
                                   (try+
                                     (corrupt-file! corruption)
                                     (catch [:type :jepsen.control/nonzero-exit
                                             :exit 2] e
                                       [:io-error (:err e)]))))))
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

    {:f     :bitflip-file-chunks
     :value [{:node        \"n3\"
              :file        \"/foo/bar\"
              :start       512
              :end         1024
              :probability 1e-3}]}

  This flips roughly one in a thousand bits, in the region of /foo/bar between
  512 and 1024 bytes. The `mod`, `i`, and `chunk-size` settings all work as
  you'd expect.

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

(defn helix-gen
  "Takes default options for a single file corruption map (see
  `corrupt-file-nemesis`), and a generator which produces operations like {:f
  :bitflip-file-chunks}. Returns a generator which fills in values for those
  operations, such that in each operation, every node corrupts the file, but
  they all corrupt different, stable regions.

    (helix-gen {:chunk-size 2}
               (gen/repeat {:type :info, :f :copy-file-chunks}))

  When first invoked, selects a permutation of the nodes in the test, assigning
  each a random index from 0 to n-1. Each value emitted by the generator uses
  that index, and `modulus` n. If the permutation of node is is [n1, n2, n3],
  and the chunk size is 2 bytes, the affected chunks look like:

    node   file bytes
           0123456789abcde ...
    n1     ╳╳    ╳╳    ╳╳
    n2       ╳╳    ╳╳    ╳
    n3         ╳╳    ╳╳

  This seems exceedingly likely to destroy a cluster, but some systems may
  survive it. In particular, systems which keep their on-disk representation
  very close across different nodes may be able to recover from the intact
  copies on other nodes."
  ([f-gen]
   (helix-gen {} f-gen))
  ([default-corruption-opts f-gen]
   (HelixGen. f-gen default-corruption-opts)))

(defrecord NodesGen [n default-opts f-gen]
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
        test ctx)))

  (update [this test ctx event]
    this))

(defn nodes-gen
  "Takes a number of nodes `n`, a map of default file corruption options (see
  `corrupt-file-nemesis`), and a generator of operations like `{:type :info, :f
  :snapshot-file-chunk}` with `nil` `:value`s. Returns a generator which fills
  in values with file corruptions, restricted to at most `n` nodes over the
  course of the test. This corrupts bytes [128, 256) on at most two nodes over
  the course of the test.

    (nodes-gen 2
              {:start 128, :end 256}
              (gen/repeat {:type :info, :f :bitflip-file-chunks}))

  `n` can also be a function `(n test)`, which can be used to select (e.g.) a
  minority of nodes. For example, try `(comp jepsen.util/minority count
  :nodes)`.

  This generator is intended to stress systems which can tolerate disk faults
  on up to n nodes, but no more."
  ([n f-gen]
   (nodes-gen n {} f-gen))
  ([n default-opts f-gen]
   (NodesGen.
     n
     default-opts
     f-gen)))
