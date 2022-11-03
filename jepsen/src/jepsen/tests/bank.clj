(ns jepsen.tests.bank
  "Helper functions for doing bank tests, where you simulate transfers between
  accounts, and verify that reads always show the same balance. The test map
  should have these additional options:

  :accounts     A collection of account identifiers.
  :total-amount Total amount to allocate.
  :max-transfer The largest transfer we'll try to execute."
  (:refer-clojure :exclude [read test])
  (:require [clojure.core.reducers :as r]
            [jepsen [checker :as checker]
             [generator :as gen]
             [history :as h]
                    [store :as store]
                    [util :as util]]
            [jepsen.checker.perf :as perf]
            [gnuplot.core :as g]))

(defn read
  "A generator of read operations."
  [_ _]
  {:type :invoke, :f :read})

(defn transfer
  "Generator of a transfer: a random amount between two randomly selected
  accounts."
  [test _]
  {:type  :invoke
   :f     :transfer
   :value {:from    (rand-nth (:accounts test))
           :to      (rand-nth (:accounts test))
           :amount  (+ 1 (rand-int (:max-transfer test)))}})

(def diff-transfer
  "Transfers only between different accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer))

(defn generator
  "A mixture of reads and transfers for clients."
  []
  (gen/mix [diff-transfer read]))

(defn err-badness
  "Takes a bank error and returns a number, depending on its type. Bigger
  numbers mean more egregious errors."
  [test err]
  (case (:type err)
    :unexpected-key (count (:unexpected err))
    :nil-balance    (count (:nils err))
    :wrong-total    (Math/abs (float (/ (- (:total err) (:total-amount test))
                                        (:total-amount test))))
    :negative-value (- (reduce + (:negative err)))))

(defn check-op
  "Takes a single op and returns errors in its balance"
  [accts total negative-balances? op]
  (let [ks       (keys (:value op))
        balances (vals (:value op))]
    (cond (not-every? accts ks)
          {:type        :unexpected-key
           :unexpected  (remove accts ks)
           :op          op}

          (some nil? balances)
          {:type    :nil-balance
           :nils    (->> (:value op)
                         (remove val)
                         (into {}))
           :op      op}

          (not= total (reduce + balances))
          {:type     :wrong-total
           :total    (reduce + balances)
           :op       op}

          (and (not negative-balances?) (some neg? balances))
          {:type     :negative-value
           :negative (filter neg? balances)
           :op       op})))

(defn checker
  "Verifies that all reads must sum to (:total test), and, unless
  :negative-balances? is true, checks that all balances are
  non-negative."
  [checker-opts]
  (reify checker/Checker
    (check [this test history opts]
      (let [accts (set (:accounts test))
            total (:total-amount test)
            reads (->> history
                       (h/filter (h/has-f? :read))
                       h/oks)
            errors (->> reads
                        (r/map (partial check-op
                                        accts
                                        total
                                        (:negative-balances? checker-opts)))
                        (r/filter identity)
                        (group-by :type))]
        {:valid?      (every? empty? (vals errors))
         :read-count  (count reads)
         :error-count (reduce + (map count (vals errors)))
         :first-error (util/min-by (comp :index :op) (map first (vals errors)))
         :errors      (->> errors
                           (map
                             (fn [[type errs]]
                               [type
                                (merge {:count (count errs)
                                        :first (first errs)
                                        :worst (util/max-by
                                                 (partial err-badness test)
                                                 errs)
                                        :last  (peek errs)}
                                       (if (= type :wrong-total)
                                         {:lowest  (util/min-by :total errs)
                                          :highest (util/max-by :total errs)}
                                         {}))]))
                           (into {}))}))))

(defn ok-reads
  "Filters a history to just OK reads. Returns nil if there are none."
  [history]
  (let [h (filter #(and (h/ok? %)
                        (= :read (:f %)))
                  history)]
    (when (seq h)
      (vec h))))

(defn by-node
  "Groups operations by node."
  [test history]
  (let [nodes (:nodes test)
        n     (count nodes)]
    (->> history
         (r/filter (comp number? :process))
         (group-by (fn [op]
                     (let [p (:process op)]
                       (nth nodes (mod p n))))))))

(defn points
  "Turns a history into a seqeunce of [time total-of-accounts] points."
  [history]
  (mapv (fn [op]
          [(util/nanos->secs (:time op))
           (reduce + (remove nil? (vals (:value op))))])
        history))

(defn plotter
  "Renders a graph of balances over time"
  []
  (reify checker/Checker
    (check [this test history opts]
      (when-let [reads (ok-reads history)]
        (let [totals (->> reads
                          (by-node test)
                          (util/map-vals points))
              colors (perf/qs->colors (keys totals))
              path (.getCanonicalPath
                     (store/path! test (:subdirectory opts) "bank.png"))
              preamble (concat (perf/preamble path)
                               [['set 'title (str (:name test) " bank")]
                                '[set ylabel "Total of all accounts"]])
              series (for [[node data] totals]
                       {:title      node
                        :with       :points
                        :pointtype  2
                        :linetype   (colors node)
                        :data       data})]
          (-> {:preamble  preamble
               :series    series}
              (perf/with-range)
              (perf/with-nemeses history (:nemeses (:plot test)))
              perf/plot!)
          {:valid? true})))))

(defn test
  "A partial test; bundles together some default choices for keys and amounts
  with a generator and checker. Options:

  :negative-balances?   if true, doesn't verify that balances remain positive"
  ([]
   (test {:negative-balances? false}))
  ([opts]
   {:max-transfer  5
    :total-amount  100
    :accounts      (vec (range 8))
    :checker       (checker/compose {:SI    (checker opts)
                                     :plot  (plotter)})
    :generator     (generator)}))
