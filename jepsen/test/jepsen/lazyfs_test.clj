(ns jepsen.lazyfs-test
  "Tests the `lazyfs` filesystem.
   
   Tries losing both fsynced and unfsynced writes.
   
   Tests using two styles of generators:

   - interleaved Client+Nemesis generator that:
       - interleaves Client writes, may be fsynced or unfsynced,
         and Nemesis losing unfsynced writes
       - followed by a final read
   - phased generator that:
       - does unfsynced writes
       - an optional fsync
       - loses unfsynced writes
       - followed by a final read
   
   A fsynced Client write is two individual commands:
   
   ```clj
   (c/exec :echo (str value \" \") :>> file)
   (c/exec :sync file)
   ```

   When using an interleaved Client+Nemesis generator with synced writes,
   the [[FileSetClient]] and [[jepsen.lazyfs/nemesis]] need to coordinate to avoid the
   Nemesis' [[jepsen.lazyfs/lose-unfsynced-writes!]] occurring between the Client's
   write and fsync. This is done by wrapping the native Nemesis with
   one that selectively `(locking fsync-lock)`s when `invoke!`ing.
   
   There is no `locking` with unfsynced writes or phased generators."
  (:require
   [clojure
    [string :as str]
    [test :refer [deftest is]]]
   [jepsen [checker :as checker]
    [client :as client]
    [control :as c]
    [core :as jepsen]
    [generator :as gen]
    [lazyfs :as lazyfs]
    [nemesis :as nemesis]
    [tests :as tests]
    [util :as u]]
   [jepsen.os.debian :as debian]))

(def dir
  "Directory to store set file."
  "/tmp/jepsen/lazyfs-test")

(def node
  "Node to run test on."
  "n1")

(defn write-ops
  "Given a `fsync-writes?` flag, returns a lazy sequence of write ops.
   Ops may contain `{:fsync? true}`."
  [fsync-writes?]
  (->> (range)
       (map (fn [value]
              (merge
               {:type  :invoke
                :f     :add
                :value value}
               (when fsync-writes?
                 {:fsync? true}))))))

(defn read-op
  "Given a `final-read?` flag, returns a read op.
   Ops may contain `{final? true}`."
  [final-read?]
  (merge
   {:type  :invoke
    :f     :read
    :value nil}
   (when final-read?
     {:final? true})))

(def fsync-op
  "An fsync op."
  {:type  :invoke
   :f     :fsync
   :value nil})

(def lose-unfsynced-writes-op
  "A `lose-unfsynced-writes` op for [[node]]."
  {:type  :info
   :f     :lose-unfsynced-writes
   :value [node]})

(def fsync-lock
  "Object to coordinate Client's write/fsync and Nemesis' `lose-unfsynced-writes`."
  (Object.))

;; Wraps a [[jepsen.lazyfs/nemesis]].
;; If `fsync?`, will `(locking fsync-lock)` to
;; not interleave between Client's write and fsync commands."
(defrecord WrappedNemesis [nemesis fsync?]
  nemesis/Nemesis
  (setup!
    [this _test]
    ; use this, so no wrapped-nemesis/setup!
    this)

  (invoke!
    [_this test {:keys [f] :as op}]
    (case f
      :lose-unfsynced-writes
      (let [v (c/on-nodes test (:value op)
                          (fn [test _node]
                            (if-not fsync?
                              (nemesis/invoke! nemesis test op)
                              (locking fsync-lock
                                (nemesis/invoke! nemesis test op)))))]
        (assoc op :value v))))

  (teardown!
    [_this _test]
    ; this did its own setup!, so no wrapped-nemesis/teardown!
    nil)

  nemesis/Reflection
  (fs
    [_this]
    #{:lose-unfsynced-writes}))

; The `FileSetClient` will fsync writes if the `op` contains `{:fsync? true}`.
(defrecord FileSetClient []
  client/Client
  (open! [this _test _node]
    (assoc this
           :file (str dir "/" (u/rand-distribution) ".set")))

  (setup! [{:keys [file] :as _this} test]
    ; insure file doesn't exist, i.e. don't reuse
    (c/with-node test node
      (c/su
       (c/exec :rm :-rf file))))

  (invoke! [{:keys [file] :as _this} test {:keys [f fsync? value] :as op}]
    (c/with-node test node
      (case f
        :add  (do
                (if-not fsync?
                  (c/exec :echo (str value " ") :>> file)
                  (locking fsync-lock
                    (c/exec :echo (str value " ") :>> file)
                    (c/exec :sync file)))
                (assoc op :type :ok))

        :read (let [vals (-> (c/exec :cat file)
                             (str/split #"\s+")
                             (->> (remove #{""})
                                  (mapv parse-long)))]
                (assoc op :type :ok, :value vals))

        :fsync (do
                 (c/exec :sync file)
                 (assoc op :type :ok)))))

  (teardown! [_this _test])

  (close! [_this _test]))

(defn interleaved-gen
  "Given a `fsync?` flag,
   returns a generator that:

   - interleaves 
       - Client writes, `fsync?` flag determines fsyncing 
       - Nemesis losing unfsynced writes
   - for 5 seconds
   - then a final read"
  [fsync?]
  (gen/phases
   (gen/log (str "Combining Client writes (fsync? " fsync? "), and Nemesis losing unfsynced writes..."))
   (->> (->> (write-ops fsync?)
             (gen/delay 1/100))
        (gen/nemesis
         (->> lose-unfsynced-writes-op
              repeat
              (gen/delay 1/2)))
        (gen/time-limit 5))

   (gen/log "Final reads...")
   (gen/clients (read-op true))))

(defn phased-gen
  "Given a `fsync?` flag,
   returns a generator that does in phases:

   - 100 writes, unfsynced
   - optionally fsyncs
   - loses unfsynced writes
   - final read"
  [fsync?]
  (gen/phases
   (gen/log "100 writes (unfsynced)...")
   (->> (write-ops false)
        (take 100)
        (gen/delay 1/100)
        (gen/clients))

   (when fsync?
     (gen/log "fsyncing")
     (gen/clients fsync-op))

   (gen/log "losing unfsynced writes")
   (gen/nemesis lose-unfsynced-writes-op)

   (gen/log "final reads...")
   (gen/clients (read-op true))))

(defn test-map
  "Given a `type`, one of `#{:interleaved :phased}`, and a `fsync?` flag,
   returns a test map that writes/read to a unique file in [[dir]],
   using `type` of generator and optionally fsyncing writes."
  [type fsync?]
  (assert (#{:interleaved :phased} type))
  (let [lazyfs-map      (lazyfs/lazyfs dir)
        wrapped-nemesis (-> lazyfs-map
                            lazyfs/nemesis
                            (WrappedNemesis. fsync?))
        generator       (case type
                          :interleaved (interleaved-gen fsync?)
                          :phased      (phased-gen      fsync?))]
    (assoc tests/noop-test
           :name      "lazyfs file set"
           :os        debian/os
           :db        (lazyfs/db lazyfs-map)
           :client    (FileSetClient.)
           :nemesis   wrapped-nemesis
           :generator generator
           :checker   (checker/set)
           :nodes     [node])))

(deftest ^:integration lazyfs-test
  (let [interleaved-unfsynced-map  (test-map :interleaved false)
        interleaved-unfsynced-test (jepsen/run! interleaved-unfsynced-map)

        interleaved-synced-map  (test-map :interleaved true)
        interleaved-synced-test (jepsen/run! interleaved-synced-map)

        phased-unfsynced-map  (test-map :phased false)
        phased-unfsynced-test (jepsen/run! phased-unfsynced-map)

        phased-synced-map  (test-map :phased true)
        phased-synced-test (jepsen/run! phased-synced-map)]

    ; unfsynced writes, interleaved generator 
    (let [{:keys [acknowledged-count attempt-count ok-count lost-count recovered-count unexpected-count valid?]}
          (-> interleaved-unfsynced-test :results)]
      (is (not valid?))
      (is (pos? acknowledged-count))
      (is (< ok-count attempt-count))
      (is (pos? ok-count))
      (is (pos? lost-count))
      (is (= 0 recovered-count unexpected-count)))

    ; unfsynced writes, phased generator
    (let [{:keys [acknowledged-count attempt-count ok-count lost-count recovered-count unexpected-count valid?]}
          (-> phased-unfsynced-test :results)]
      (is (not valid?))
      (is (= acknowledged-count attempt-count lost-count))
      (is (= 0 ok-count recovered-count unexpected-count)))

    ; fsynced writes, interleaved generator
    (let [{:keys [acknowledged-count attempt-count ok-count lost-count recovered-count unexpected-count valid?]}
          (-> interleaved-synced-test :results)]
      (is valid?)
      (is (= acknowledged-count attempt-count ok-count))
      (is (= 0 lost-count recovered-count unexpected-count)))

    ; unfsynced writes, phased generator
    (let [{:keys [acknowledged-count attempt-count ok-count lost-count recovered-count unexpected-count valid?]}
          (-> phased-synced-test :results)]
      (is valid?)
      (is (= acknowledged-count attempt-count ok-count))
      (is (= 0 lost-count recovered-count unexpected-count)))))
