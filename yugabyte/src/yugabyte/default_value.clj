(ns yugabyte.default-value
  "This test is designed to stress a specific part of YugaByte DB's
  non-transactional DDL to expose anomalies in DML. We simulate a migration
  in which a user wishes to add a column with a default value of `0` to an
  existing table, and execute concurrent inserts into the table, and
  concurrent reads, looking for cases where the column exists, but its value
  is `null` instead."
  (:require [jepsen [checker :as checker]
                    [generator :as gen]]
            [knossos.op :as op]))

(defn r [_ _]            {:type :invoke, :f :read})
(defn i [_ _]            {:type :invoke, :f :insert})
(defn create-table [_ _] {:type :invoke, :f :create-table})
(defn drop-table   [_ _] {:type :invoke, :f :drop-table})
(defn add-column   [_ _] {:type :invoke, :f :add-column, :value "v"})
(defn drop-column  [_ _] {:type :invoke, :f :drop-column, :value "v"})

(defn generator
  []
  (->> (gen/mix (vec (concat [create-table drop-table]
                             ; YB doesn't support adding columns with defaults
                             ; yet, so we create/drop tables instead.
                             ; [add-column drop-column]
                             (take 50 (cycle [r i])))))
       (gen/stagger 1/100)))

(defn bad-row
  "Is this particular row illegal--e.g. does it contain a `null`? Returns row
  if true."
  [row]
  (and (seq (filter nil? (vals row)))
       row))

(defn bad-table
  "Does this collection of rows have a bad row in it? If so, returns that row,
  otherwise nil."
  [table]
  (seq (filter bad-row table)))

(defn bad-read
  "Does this read op have a bad row in it? Returns that row if so, otherwise
  nil."
  [op]
  (bad-table (:value op)))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [reads (->> history
                       (filter op/ok?)
                       (filter (comp #{:read} :f)))
            bad   (filter bad-read reads)]
        {:valid?          (empty? bad)
         :read-count      (count reads)
         :bad-read-count  (count bad)
         :bad-reads       (map (fn [op]
                                 {:op       op
                                  :bad-rows (keep bad-row (:value op))})
                               bad)}))))

(defn workload
  [opts]
  {:checker   (checker)
   :generator (generator)})
