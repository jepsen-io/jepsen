(ns jepsen.nemesis
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [fipp.ednize :as fipp.ednize]
            [jepsen [client   :as client]
                    [control  :as c]
                    [net      :as net]
                    [util     :as util]]
            [jepsen.control [util :as cu]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defprotocol Nemesis
  (setup! [this test] "Set up the nemesis to work with the cluster. Returns the
                      nemesis ready to be invoked")
  (invoke! [this test op] "Apply an operation to the nemesis, which alters the
                          cluster.")
  (teardown! [this test] "Tear down the nemesis when work is complete"))

(defprotocol Reflection
  "Optional protocol for reflecting on nemeses."
  (fs [this] "What :f functions does this nemesis support? Returns a set.
             Helpful for composition."))

(extend-protocol fipp.ednize/IOverride (:on-interface Nemesis))
(extend-protocol fipp.ednize/IEdn (:on-interface Nemesis)
  (-edn [gen]
    (if (record? gen)
      ; Ugh this is such a hack but Fipp's extension points are sort of a
      ; mess--you can't override the document generation behavior on a
      ; per-class basis. Probably sensible for perf reasons, but makes our life
      ; hard.
      ;
      ; Convert records (which respond to map?) into (RecordName {:some map
      ; ...), so they pretty-print with fewer indents.
      (list (symbol (.getName (class gen)))
            (into {} gen))
      ; We can't return a reify without entering an infinite loop (ugh) so uhhh
      (tagged-literal 'unprintable (str gen))
      )))

(def noop
  "Does nothing."
  (reify Nemesis
    (setup! [this test] this)
    (invoke! [this test op] op)
    (teardown! [this test] this)
    Reflection
    (fs [this] #{})))

(defrecord Validate [nemesis]
  Nemesis
  (setup! [this test]
    (let [res (setup! nemesis test)]
      (when-not (satisfies? Nemesis res)
        (throw+ {:type ::setup-returned-non-nemesis
                 :got  res}
                nil
                "expected setup! to return a Nemesis, but got %s instead"
                (pr-str res)))
      (Validate. res)))

  (invoke! [this test op]
    (let [op' (invoke! nemesis test op)
          problems
          (cond-> []
            (not (map? op'))
            (conj "should be a map")

            (not (#{:info} (:type op')))
            (conj ":type should be :info")

            (not= (:process op) (:process op'))
            (conj ":process should be the same")

            (not= (:f op) (:f op'))
            (conj ":f should be the same"))]
      (when (seq problems)
        (throw+ {:type      ::invalid-completion
                 :op        op
                 :op'       op'
                 :problems  problems}))
      op'))

  (teardown! [this test]
    (teardown! nemesis test)))

(defn validate
  "Wraps a nemesis, validating that it constructs responses to setup and invoke
  correctly."
  [nemesis]
  (Validate. nemesis))

(defn timeout
  "Sometimes nemeses are unreliable. If you wrap them in this nemesis, it'll
  time out their operations with the given timeout, in milliseconds. Timed out
  operations have :value :timeout."
  [timeout-ms nemesis]
  (reify Nemesis
    (setup! [this test]
      (timeout timeout-ms (setup! nemesis test)))

    (invoke! [this test op]
      (util/timeout timeout-ms (assoc op :value :timeout)
                    (invoke! nemesis test op)))

    (teardown! [this test]
      (teardown! nemesis test))))

(defn bisect
  "Given a sequence, cuts it in half; smaller half first."
  [coll]
  (split-at (Math/floor (/ (count coll) 2)) coll))

(defn split-one
  "Split one node off from the rest"
  ([coll]
   (split-one (rand-nth coll) coll))
  ([loner coll]
    [[loner] (remove (fn [x] (= x loner)) coll)]))

(defn complete-grudge
  "Takes a collection of components (collections of nodes), and computes a
  grudge such that no node can talk to any nodes outside its partition."
  [components]
  (let [components (map set components)
        universe   (apply set/union components)]
    (reduce (fn [grudge component]
              (reduce (fn [grudge node]
                        (assoc grudge node (set/difference universe component)))
                      grudge
                      component))
            {}
            components)))

(defn invert-grudge
  "Takes a universe of nodes and a map of nodes to nodes they should be
  connected to, and returns a map of nodes to nodes they should NOT be
  connected to."
  [nodes conns]
  (let [nodes (set nodes)]
    (->> nodes
         (map (fn [a] [a (set/difference nodes (conns a #{}))]))
         (into (sorted-map)))))

(defn bridge
  "A grudge which cuts the network in half, but preserves a node in the middle
  which has uninterrupted bidirectional connectivity to both components."
  [nodes]
  (let [components (bisect nodes)
        bridge     (first (second components))]
    (-> components
        complete-grudge
        ; Bridge won't snub anyone
        (dissoc bridge)
        ; Nobody hates the bridge
        (->> (util/map-vals #(disj % bridge))))))

(defn partitioner
  "Responds to a :start operation by cutting network links as defined by
  (grudge nodes), and responds to :stop by healing the network. The grudge to
  apply is either taken from the :value of a :start op, or if that is nil, by
  calling (grudge (:nodes test))"
  ([] (partitioner nil))
  ([grudge]
   (reify Nemesis
     (setup! [this test]
       (net/heal! (:net test) test)
       this)

     (invoke! [this test op]
       (case (:f op)
         :start (let [grudge (or (:value op)
                                 (if grudge
                                   (grudge (:nodes test))
                                   (throw (IllegalArgumentException.
                                            (str "Expected op " (pr-str op)
                                                 " to have a grudge for a :value, but none given.")))))]
                  (net/drop-all! test grudge)
                  (assoc op :value [:isolated grudge]))
         :stop  (do (net/heal! (:net test) test)
                    (assoc op :value :network-healed))))

     (teardown! [this test]
       (net/heal! (:net test) test)))))

(defn partition-halves
  "Responds to a :start operation by cutting the network into two halves--first
  nodes together and in the smaller half--and a :stop operation by repairing
  the network."
  []
  (partitioner (comp complete-grudge bisect)))

(defn partition-random-halves
  "Cuts the network into randomly chosen halves."
  []
  (partitioner (comp complete-grudge bisect shuffle)))

(defn partition-random-node
  "Isolates a single node from the rest of the network."
  []
  (partitioner (comp complete-grudge split-one)))

(defn majorities-ring-perfect
  "The perfect variant of majorities-ring, used for 5-node clusters."
  [nodes]
  (let [U (set nodes)
        n (count nodes)
        m (util/majority n)]
    (->> nodes
         shuffle                ; randomize
         cycle                  ; form a ring
         (partition m 1)        ; construct majorities
         (take n)               ; one per node
         (map (fn [majority]    ; invert into connections to *drop*
                [(nth majority (Math/floor (/ (count majority) 2)))
                 (set/difference U (set majority))]))
         (into {}))))

(defn majorities-ring-stochastic
  "The stochastic variant of majorities-ring, used for larger clusters."
  [nodes]
  (let [n (count nodes)
        m (util/majority n)
        U (set nodes)]
    (loop [; We're going to build up a connection graph incrementally
           conns (->> nodes (map (juxt identity hash-set)) (into {}))
           ; By connecting the least-connected nodes to other least-connected
           ; nodes.
           by-degree (sorted-map 1 (set nodes))]
      (let [; Construct a shuffled, in-degree-order seq of [degree, node] pairs
            dns (mapcat (fn [[degree nodes]]
                          (map vector (repeat degree) (shuffle nodes)))
                        by-degree)
            ; Pick a node `a` with minimal degree.
            [a-degree a] (first dns)]
        (if (<= m a-degree)
          ; Every node has a majority
          (invert-grudge nodes conns)

          ; Link a to some other minimally-connected node b which a is *not*
          ; already connected to.
          (let [[b-degree b] (->> dns
                                  (remove (comp (conns a) second))
                                  first)
                ; Link a to b
                conns' (-> conns
                            (update a conj b)
                            (update b conj a))
                ; Conj which creates a set if necessary
                conj-set (fn [s x] (if s (conj s x) #{x}))
                ; And increment their counts
                ad' (inc a-degree)
                bd' (inc b-degree)
                by-degree' (-> by-degree
                               (update a-degree disj a)
                               (update b-degree disj b)
                               (update ad' conj-set a)
                               (update bd' conj-set b))]
            (recur conns' by-degree')))))))

(defn majorities-ring
  "A grudge in which every node can see a majority, but no node sees the *same*
  majority as any other. There are nice, exact solutions where the topology
  *does* look like a ring: these are possible for 4, 5, 6, 8, etc nodes. Seven,
  however, does *not* work so cleanly--some nodes must be connected to *more*
  than four others. We therefore offer two algorithms: one which provides an
  exact ring for 5-node clusters (generally common in Jepsen), and a stochastic
  one which doesn't guarantee efficient ring structures, but works for larger
  clusters.

  Wow this actually is *shockingly* complicated. Wonder if there's a better
  way?"
  [nodes]
  (if (<= (count nodes) 5)
    (majorities-ring-perfect nodes)
    (majorities-ring-stochastic nodes)))

(defn partition-majorities-ring
  "Every node can see a majority, but no node sees the *same* majority as any
  other. Randomly orders nodes into a ring."
  []
  (partitioner majorities-ring))

(declare f-map)

(defrecord FMap [lift unlift nem]
  Nemesis
  (setup! [this test]
    (f-map lift (setup! nem test)))

  (invoke! [this test op]
    (-> nem
        (invoke! test (update op :f unlift))
        (update :f lift)))

  (teardown! [this test]
    (teardown! nem test))

  Reflection
  (fs [this]
    (set (map lift (fs nem)))))

(defn f-map
  "Remaps the :f values that a nemesis accepts. Takes a function (presumably
  injective) which transforms `:f` values: `(lift f) -> g`, and a nemesis which
  accepts operations like `{:f f}`. The nemesis must support Reflection/fs.
  Returns a new nemesis which takes `{:f g}` instead. For example:

    (f-map (fn [f] [:foo f]) (partition-random-halves))

  ... yields a nemesis which takes ops like `{:f [:foo :start] ...}` and calls
  the underlying partitioner nemesis with `{:f :start ...}`. This is designed
  for symmetry with generator/f-map, so you can say:

    (gen/f-map lift gen)
    (nem/f-map lift gen)

  and get a generator and nemesis that work together. Particularly handy for
  building up complex nemesis packages using nemesis.combined!

  If you know all of your fs in advance, you can also do this with `compose`,
  but it turns out to be handy to have this as a separate function."
  [lift nem]
  ; Construct the inverse of `lift` by materializing the whole f domain
  ; into a map.
  (let [fs (fs nem)
        unlift (zipmap (map lift fs) fs)]
    (FMap. lift unlift nem)))

(declare compose)

; This version of a composed nemesis uses the Reflection protocol to identify
; which fs map to which nemeses. fm is a map of fs to indices in the nemesis
; vector--this cuts down on pprint amplification.
(defrecord ReflCompose [fm nemeses]
  Nemesis
  (setup! [this test]
    (compose (map #(setup! % test) nemeses)))

  (invoke! [this test op]
    (if-let [n (nth nemeses (get fm (:f op)))]
      (invoke! n test op)
      (throw (IllegalArgumentException.
               (str "No nemesis can handle :f " (pr-str (:f op))
                    " (expected one of " (pr-str (keys fm)) ")")))))

  (teardown! [this test]
    (mapv #(teardown! % test) nemeses))

  Reflection
  (fs [this]
    (reduce into #{} (map fs nemeses))))

; This version of a composed nemesis uses an explicit map of fs to nemeses.
(defrecord MapCompose [nemeses]
  Nemesis
  (setup! [this test]
    (compose (util/map-vals #(setup! % test) nemeses)))

  (invoke! [this test op]
    (let [f (:f op)]
      (loop [nemeses nemeses]
        (if-not (seq nemeses)
          (throw (IllegalArgumentException.
                   (str "no nemesis can handle " (:f op))))
          (let [[fs- nemesis] (first nemeses)]
            (if-let [f' (fs- f)]
              (assoc (invoke! nemesis test (assoc op :f f')) :f f)
              (recur (next nemeses))))))))

  (teardown! [this test]
    (util/map-vals #(teardown! % test) nemeses))

  Reflection
  (fs [this]
    (->> (keys nemeses)
         (mapcat (fn [f-map]
                   (cond (map? f-map) (keys f-map)
                         (set? f-map) f-map
                         true
                         (throw+ {:type    ::can't-infer-fs
                                  :message "We can only infer fs from compose nemeses built with maps or sets as their f mapping objects."}))))
         (into #{}))))

(defn compose
  "Combines multiple Nemesis objects into one. If all, or all but one, nemesis
  support Reflection, compose can simply take a collection of nemeses, and use
  (fs nem) to figure out what ops to send to which nemesis. Otherwise...

  Takes a map of fs to nemeses and returns a single nemesis which, depending
  on (:f op), routes to the appropriate child nemesis. `fs` should be a
  function which takes (:f op) and returns either nil, if that nemesis should
  not handle that :f, or a new :f, which replaces the op's :f, and the
  resulting op is passed to the given nemesis. For instance:

      (compose {#{:start :stop} (partition-random-halves)
                #{:kill}        (process-killer)})

  This routes `:kill` ops to process killer, and :start/:stop to the
  partitioner. What if we had two partitioners which *both* take :start/:stop?

      (compose {{:split-start :start
                 :split-stop  :stop} (partition-random-halves)
                {:ring-start  :start
                 :ring-stop2  :stop} (partition-majorities-ring)})

  We turn :split-start into :start, and pass that op to
  partition-random-halves."
  [nemeses]
  (if (map? nemeses)
    (MapCompose. nemeses)
    ; A collection; use reflection to compute a map of :fs to nemeses.
    (let [nemeses (vec nemeses)
          [_ fm]
          (reduce (fn [[i fm] n]
                    ; For the i'th nemesis, our fmap is...
                    [(inc i)
                     (reduce (fn [fm f]
                               (assert (not (get fm f))
                                       (str "Nemeses " (pr-str n) " and "
                                            (pr-str (get fm f))
                                            " are mutually incompatible;"
                                            " both use :f " (pr-str f)))
                               (assoc fm f i))
                             fm
                             (fs n))])
                  [0 {}]
                  nemeses)]
      (ReflCompose. fm nemeses))))

(defn set-time!
  "Set the local node time in POSIX seconds."
  [t]
  (c/su (c/exec :date "+%s" :-s (str \@ (long t)))))

(defn clock-scrambler
  "Randomizes the system clock of all nodes within a dt-second window."
  [dt]
  (reify Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (assoc op :value
             (c/with-test-nodes test
               (set-time! (+ (/ (System/currentTimeMillis) 1000)
                             (- (rand-int (* 2 dt)) dt))))))

    (teardown! [this test]
      (c/with-test-nodes test
        (set-time! (/ (System/currentTimeMillis) 1000))))))

(defn node-start-stopper
  "Takes a targeting function which, given a list of nodes, returns a single
  node or collection of nodes to affect, and two functions `(start! test node)`
  invoked on nemesis start, and `(stop! test node)` invoked on nemesis stop.
  Returns a nemesis which responds to :start and :stop by running the start!
  and stop! fns on each of the given nodes. During `start!` and `stop!`, binds
  the `jepsen.control` session to the given node, so you can just call `(c/exec
  ...)`.

  The targeter can take either (targeter test nodes) or, if that fails,
  (targeter nodes).

  Re-selects a fresh node (or nodes) for each start--if targeter returns nil,
  skips the start. The return values from the start and stop fns will become
  the :values of the returned :info operations from the nemesis, e.g.:

      {:value {:n1 [:killed \"java\"]}}"
  [targeter start! stop!]
  (let [nodes (atom nil)]
    (reify Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (locking nodes
          (assoc op :type :info, :value
                 (case (:f op)
                   :start (let [ns (:nodes test)
                                ns (try (targeter test ns)
                                        (catch clojure.lang.ArityException e
                                          (targeter ns)))
                                ns (util/coll ns)]
                            (if ns
                              (if (compare-and-set! nodes nil ns)
                                (c/on-many ns (start! test c/*host*))
                                (str "nemesis already disrupting "
                                     (pr-str @nodes)))
                              :no-target))
                   :stop (if-let [ns @nodes]
                           (let [value (c/on-many ns (stop! test c/*host*))]
                             (reset! nodes nil)
                             value)
                           :not-started)))))

      (teardown! [this test]))))

(defn hammer-time
  "Responds to `{:f :start}` by pausing the given process name on a given node
  or nodes using SIGSTOP, and when `{:f :stop}` arrives, resumes it with
  SIGCONT.  Picks the node(s) to pause using `(targeter list-of-nodes)`, which
  defaults to `rand-nth`. Targeter may return either a single node or a
  collection of nodes."
  ([process] (hammer-time rand-nth process))
  ([targeter process]
   (node-start-stopper targeter
                       (fn start [t n]
                         (c/su (c/exec :killall :-s "STOP" process))
                         [:paused process])
                       (fn stop [t n]
                         (c/su (c/exec :killall :-s "CONT" process))
                         [:resumed process]))))

(defn truncate-file
  "A nemesis which responds to
  ```clj
  {:f     :truncate
   :value {\"some-node\" {:file \"/path/to/file or /path/to/dir\"
                        :drop 64}}}
  ```
  where the value is a map of nodes to `{:file, :drop}` maps, on those nodes,
  drops the last `:drop` bytes from the given file, or a random file from the given directory."
  []
  (reify Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assert (= (:f op) :truncate))
      (let [plan (:value op)]
        (->> (c/on-nodes test
                         (keys plan)
                         (fn [_ node]
                           (let [{:keys [file drop]} (plan node)
                                 _ (assert (string? file))
                                 _ (assert (integer? drop))
                                 file (if (cu/file? file)
                                        file
                                        (rand-nth (cu/ls-full file)))]
                             (c/su
                              (c/exec :truncate :-c :-s (str "-" drop) file))
                             {:file file :drop drop})))
             (assoc op :value))))

    (teardown! [this test])

    Reflection
    (fs [this]
      #{:truncate})))

(def bitflip-dir
  "Where do we install the bitflip utility?"
  "/opt/jepsen/bitflip")

(defrecord Bitflip []
  Nemesis
  (setup! [this test]
    (c/with-test-nodes test
      (c/su
        (cu/install-archive! "https://github.com/aybabtme/bitflip/releases/download/v0.2.0/bitflip_0.2.0_Linux_x86_64.tar.gz" bitflip-dir))
      this)
    this)

  (invoke! [this test {:keys [value] :as op}]
    (->> (c/on-nodes test (keys value)
                     (fn flip [test node]
                       (let [{:keys [file probability]} (get value node)
                             _ (when-not file
                                 (throw+ {:type ::no-file}))
                             file (if (cu/file? file)
                                    file
                                    (rand-nth (cu/ls-full file)))
                             probability (or probability 0.01)
                             percent (* 100 probability)]
                         (c/su
                           (c/exec (str bitflip-dir "/bitflip")
                                   :spray
                                   (format "percent:%.32f" percent)
                                  file))
                         {:file file :probability probability})))
         (assoc op :value)))

  (teardown! [this test])

  Reflection
  (fs [this]
    #{:bitflip}))

(defn bitflip
  "A nemesis which introduces random bitflips in files. Takes operations like:
  ```clj
  {:f     :bitflip
   :value {\"some-node\" {:file         \"/path/to/file or /path/to/dir\"
                        :probability  1e-3}}}
  ```
  This flips 1 x 10^-3 of the bits in `/path/to/file`, or a random file in `/path/to/dir`, on \"some-node\"."
  []
  (Bitflip.))
