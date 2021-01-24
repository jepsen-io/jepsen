(ns jecci.interface.nemesis
  (:require
   [jecci.interface.resolver :as r]
   [clojure.tools.logging :refer [info warn error]]))

(defn just-return-nil [& args] nil)

; Specification for how to render operations in plots
; see postgres.nemesis/plot-spec for example
; optional
(def plot-spec (r/resolve! "nemesis" "plot-spec") )

; available nemesis types, should be a set
; EX: #{:kill-sth :partition}
(def nemesis-specs (r/resolve! "nemesis" "nemesis-specs"))

; should be in the form of [[] [:kill-sth] ... [:kill-sth :partition]...]
; optional
(def all-nemeses (r/resolve! "nemesis" "all-nemeses"))

; should be in the form of [[] [:kill-sth] ... [:kill-sth :partition]...]
; optional
(def quick-nemeses (r/resolve! "nemesis" "quick-nemeses"))

; should be in the form of [[] [:kill-sth] ... [:kill-sth :partition]...]
; optional
(def very-quick-nemeses (r/resolve! "nemesis" "very-quick-nemeses"))

; should be a dict of sets mapping to corresponding functions
; functions must be an implementation of jepsen.nemesis/Nemesis
(def nemesis-composition (r/resolve! "nemesis" "nemesis-composition"))

(def op2heal-dict (r/resolve! "nemesis" "op2heal-dict"))

; optional
; nemeses that the dbms can autorecover
(def autorecover-ops (r/resolve! "nemesis" "autorecover-ops"))

; dict that maps nemeses to a heal which will be executed eventually
(def final-gen-dict (r/resolve! "nemesis" "final-gen-dict"))

; optional
; takes a nemesis type, like "kill-sth", and return an implementation
; of jepsen.generator/Generator
(def special-full-generator
  (if-let [gen (r/resolve! "nemesis" "special-full-generator")] gen
          just-return-nil))
