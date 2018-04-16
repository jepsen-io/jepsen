(ns jepsen.tests.bank
  "Helper functions for doing bank tests, where you simulate transfers between
  accounts, and verify that reads always show the same balance. The test map
  should have these additional options:

  :accounts     A collection of account identifiers.
  :total-amount Total amount to allocate.
  :max-transfer The largest transfer we'll try to execute."
  (:refer-clojure :exclude [read test])
  (:require [knossos.op :as op]
            [clojure.core.reducers :as r]
            [jepsen [generator :as gen]
                    [checker :as checker]
                    [store :as store]
                    [util :as util]]
            [jepsen.checker.perf :as perf]
            [knossos.history :as history]
            [gnuplot.core :as g]))

(defn read
  "A generator of read operations."
  [_ _]
  {:type :invoke, :f :read})

(defn transfer
  "Generator of a transfer: a random amount between two randomly selected
  accounts."
  [test process]
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

(defn checker
	"Balances must all be non-negative and sum to (:total test)."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [accts     (set (:accounts test))
            reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %))))
            errors (->> reads
                        (r/map (fn [op]
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

                                         (not= (:total-amount test)
                                               (reduce + balances))
                                         {:type     :wrong-total
                                          :total    (reduce + balances)
                                          :op       op}

                                         (some neg? balances)
                                         {:type     :negative-value
                                          :negative (filter neg? balances)
                                          :op       op}))))
                        (r/filter identity)
                        (group-by :type))]
        {:valid?      (every? empty? (vals errors))
         :read-count  (count (into [] reads))
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
  (->> history
       (r/filter op/ok?)
       (r/filter #(= :read (:f %)))
       (r/map (fn [op]
                [(util/nanos->secs (:time op))
                 (reduce + (remove nil? (vals (:value op))))]))
       (into [])))

(defn plotter
  "Renders a graph of balances over time"
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [totals (->> history
                        (by-node test)
                        (util/map-vals points))
            colors (perf/qs->colors (keys totals))
            path (.getCanonicalPath
                   (store/path! test (:subdirectory opts) "bank.png"))]
        (try
          (g/raw-plot!
            (concat (perf/preamble path)
                    [['set 'title (str (:name test) " bank")]
                     '[set ylabel "Total of all accounts"]
                     ['plot (apply g/list
                                   (for [[node points] totals]
                                     ["-"
                                      'with       'points
                                      'pointtype  2
                                      'linetype   (colors node)
                                      'title      (name node)]))]])
            (vals totals))
          {:valid? true}
          (catch java.io.IOException _
                 (throw (IllegalStateException. "Error rendering plot, verify gnuplot is installed and reachable"))))))))

(defn test
  "A partial test; bundles together some default choices for keys and amounts
  with a generator and checker."
  []
  {:max-transfer  5
   :total-amount  100
   :accounts      (vec (range 8))
   :checker       (checker/compose {:SI    (checker)
                                    :plot  (plotter)})
   :generator     (generator)})
