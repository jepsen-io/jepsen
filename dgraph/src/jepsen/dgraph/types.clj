(ns jepsen.dgraph.types
  "Quick test to demonstrate type safety & integer overflow issues."
  (:require [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [assert+]]
            [knossos.op :as op]
            [jepsen.dgraph.client :as c]
            [jepsen [client :as client]
                    [checker :as checker]]
            [jepsen.generator.pure :as gen]))

(defn hex
  "Turns a number into a hex string."
  [x]
  (str ;"0x"
       (format "%x"
               (condp instance? x
                 clojure.lang.BigInt (biginteger x)
                 x))))

; Entities is an atom of a set of entities we've written
(defrecord Client [conn entities]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "key:   int @index(int) . \n"
                               "int64: int .\n")))

  (invoke! [this test op]
    (c/with-conflict-as-fail op
      (c/with-txn [t conn]
        ; We take entity-attribute-value triples
        (let [[e a v] (:value op)]
          (case (:f op)
            :write (let [e (-> (c/set-nquads!
                                 t (str "_:e <" a "> \"" v "\" .\n"))
                               first
                               val)]
                     (swap! entities conj e)
                     (assoc op
                            :type :ok
                            :value [e a v]))
            :read  (->> (c/query t (str "{ q(func: uid($entity)) { " a " }}")
                                 {:entity e})
                        :q
                        (map (keyword a))
                        first
                        (vector e a)
                        (assoc op :type :ok, :value)))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn checker
  "Checks to make sure that things we write come back out the same way"
  []
  (reify checker/Checker
    (check [this test history opts]
      ; First pass: figure out what we supposedly wrote to the DB
      (let [state (->> history
                       (r/filter op/ok?)
                       (r/filter (comp #{:write} :f))
                       (reduce (fn [state op]
                                 (let [[e a v] (:value op)]
                                   (assert+ (not (get state [e a])))
                                   (assoc state [e a] v)))
                               {}))

            ; Make sure all writes have a successful read
            unread (->> history
                        (r/filter op/ok?)
                        (r/filter (comp #{:read} :f))
                        (r/map :value)
                        (r/map #(subvec % 0 2))
                        (into #{})
                        (set/difference (set (keys state)))
                        sort)

            ; Third pass: construct a read state
            read (->> history
                      (r/filter op/ok?)
                      (r/filter (comp #{:read} :f))
                      (r/map :value)
                      (reduce (fn [state [e a v]]
                                ; Make sure we always read the same thing
                                (assert+ (= v (get state [e a] v)))
                                (assoc state [e a] v))
                              {}))

            ; Zip writes and reads together to make a map of attributes to
            ; inputs to outputs.
            mapping (->> (keys state)
                         (reduce (fn [mapping [e a :as k]]
                                   (let [in  (get state k)
                                         out (get read  k)]
                                     (let [am (get mapping a (sorted-map))]
                                       (assoc mapping a (assoc am in out)))))
                                 (sorted-map)))
            errs (->> history
                      (r/filter op/ok?)
                      (r/filter (comp #{:read} :f))
                      (reduce (fn [errs op]
                                (let [[e a v] (:value op)]
                                  (if (= v (get state [e a]))
                                    errs
                                    (conj errs
                                          {:entity    e
                                           :attribute a
                                           :wrote     (get state [e a])
                                           :read      v}))))
                              []))]
        {:valid?      (cond (seq errs)      false
                            (seq unread)    :unknown
                            true            true)
         :error-count   (count errs)
         :unread-count  (count unread)
         :errors        (distinct errs)
         :unread        unread
         :mapping       mapping}))))

(defn nsect
  "Computes a range of n equally spaced numbers between lower and upper,
  including lower and upper."
  [n lower upper]
  (range lower (inc upper) (/ (- upper lower) (dec n))))

(def cases
  "A collection of [attribute value] triples to write and read back"
  (for [a ["foo" "int64"]
        v (->> [0
                Byte/MAX_VALUE
                Short/MAX_VALUE
                Integer/MAX_VALUE
                Long/MAX_VALUE
                16777217          ; largest exact-float-representable int
                9007199254740993  ; largest exact-double-representable int
                (* 3 (bigint Long/MAX_VALUE)) ; Well outside signed longs
                ]
               ; Around these interesting points, generate 17-number ranges
               ; around + and - x.
               (mapcat (fn [x]
                         (let [x (bigint x)]
                           (concat (range (-   x 8) (+    x  8))
                                   (range (- 0 x 8) (+ (- x) 8))))))
               ; Searching for negative overflow
               ; 9223372036852621779N
               ; 9223372036854758853N
               ; 9223372036854775264N
               ; 9223372036854775814N
               (concat (distinct (map bigint (nsect 16
                                                    9223372036854775293N
                                                    9223372036854775299N)))))]
    [a v]))

(defn workload
  "Client, generator, checker, etc"
  [opts]
  (let [entities (atom (sorted-set))]
    {:checker   (checker)
     :generator (gen/phases (gen/log "Starting")
                            (->> cases
                                 (map (fn [[a v]]
                                        {:type  :invoke
                                         :f     :write
                                         :value [nil a v]}))
                                 (gen/stagger 1/10))
                            (gen/log "Waiting to read")
                            (gen/sleep 10)
                            (delay
                              (info "Need to read entities" @entities)
                              (->> (for [e @entities
                                         a (distinct (map first cases))]
                                     {:type   :invoke
                                      :f      :read
                                      :value  [e a nil]})
                                   (repeat 3) ; sigh, dgraph likes to stop
                                   ; taking writes just cuz
                                   (apply concat)
                                   shuffle
                                   (gen/stagger 1/10))))
     :client (Client. nil entities)}))
