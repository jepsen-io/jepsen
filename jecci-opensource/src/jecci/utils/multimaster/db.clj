(ns jecci.utils.multimaster.db
  (:require [jepsen.nemesis :as nemesis]
            [jecci.interface.resolver :as r]))

; Might be useful for multimaster system
; Will improve later

(defn op
  "Shorthand for constructing a nemesis op"
  ([f]
   (op f nil))
  ([f v]
   {:type :info, :f f, :value v})
  ([f v & args]
   (apply assoc (op f v) args)))

(defmacro partition-leaders-gen
  "A generator for a partition that isolates the leaders in a
  minority."
  [get-leaders-node get-followers-node get-leaders-cnt]
  `(let [leaders (~@get-leaders-node test)
        followers (shuffle (~@get-followers-node test leaders))
        nodes       (into [] (concat leaders followers))
        components  (split-at (~@get-leaders-cnt) nodes) ; Maybe later rand(n/2+1?)
        grudge      (nemesis/complete-grudge components)]
    (op :start-partition, grudge, :partition-type :multiple-leaders)))