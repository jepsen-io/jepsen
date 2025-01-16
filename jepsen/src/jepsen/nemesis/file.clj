(ns jepsen.nemesis.file
  "Fault injection involving files on disk."
  (:require [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]]))

(defn corrupt-file!
  "Calls corrupt-file on the currently bound remote node, taking a map of
  options."
  [{:keys [file start end chunk-size mod i]}]
  (assert (string? file))
  (c/su
    (c/exec (str nemesis/bin-dir "/corrupt-file")
            (when start [:--start start])
            (when end [:--end end])
            (when chunk-size [:--chunk-size chunk-size])
            (when mod [:--mod mod])
            (when i [:--index i])
            file)))

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
      (nemesis/compile-c-resource! "corrupt-file.c" "corrupt-file"))
    this)

  (invoke! [this test {:keys [f value] :as op}]
    (assert (every? string? (map :node value)))
    (case f
      :corrupt-file-chunks
      (let [v (c/on-nodes
                test (distinct (map :node value))
                (fn node [test this-node]
                  ; Apply each relevant corruption
                  (->> value
                       (keep (fn corruption [corruption]
                               (let [{:keys [node file chunk-size mod i]
                                      :as corruption}
                                     (merge default-opts corruption)]
                                 (when (= this-node node)
                                   (corrupt-file! corruption)))))
                       (into []))))]
        (assoc op :value v))))

  (teardown! [this test]))

(defn corrupt-file-nemesis
  "This nemesis takes operations like

    {:f     :corrupt-file-chunks
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
  starting with (zero-indexed) chunk 2: 2, 7, 12, 17, .... Data is drawn from
  other chunks in the region which are *not* interfered with by this command,
  unless mod is 1, in which case chunks are randomly selected. The idea is that
  this gives us a chance to produce valid-looking structures which might be
  dereferenced by later pointers.

  All options are optional, except for :node and :file. See
  resources/corrupt-file.c for defaults.

  This function can take an optional map with defaults for each file-corruption
  operation."
  ([]
   (corrupt-file-nemesis {}))
  ([default-opts]
   (CorruptFileNemesis. default-opts)))

(defrecord CorruptFileChunksHelixGen [default-opts]
  gen/Generator
  (op [this test ctx]
    ; We immediately unfurl into an endless sequence of identical file
    ; corruptions.
    (let [nodes (:nodes test)]
      (gen/op (gen/repeat
                {:type  :info
                 :f     :corrupt-file-chunks
                 :value (mapv (fn per-node [i node]
                                (assoc default-opts
                                       :node node
                                       :mod  (count nodes)
                                       :i    i))
                              (range)
                              (shuffle nodes))})
              test ctx)))

  (update [this test ctx op]
    this))

(defn corrupt-file-chunks-helix-gen
  "Generates corrupt-file-chunks operations in a 'helix' around the cluster.
  Once per test, picks random `i`s for the nodes in the test. Takes default
  options for file corruptions, as per `corrupt-file-nemesis`. Emits a series
  of `corrupt-file-chunks` operations, where each operation has a file
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
   (corrupt-file-chunks-helix-gen {}))
  ([default-opts]
   (CorruptFileChunksHelixGen. default-opts)))
