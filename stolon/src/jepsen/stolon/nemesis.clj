(ns jepsen.stolon.nemesis
  "Nemeses for Stolon"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [nemesis :as n]
                    [net :as net]
                    [util :as util]]
            [jepsen.generator :as gen]
            [jepsen.nemesis [combined :as nc]
                            [time :as nt]]))

(defn nemesis-package
  "Constructs a nemesis and generators for Stolon."
  [opts]
  (let [opts (update opts :faults set)]
    (nc/nemesis-package opts)))
