(defproject jepsen.txn "0.1.3-SNAPSHOT"
  :description "Library for generating and analyzing multi-object transactional histories"
  :url "https://github.com/jepsen-io/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :test-selectors {:default     (fn [m] (not (or (:perf m))))
                   :all         (fn [m] true)
                   :perf        :perf}
  :dependencies [[dom-top "1.0.8"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.1"]
                                  [criterium "0.4.5"]]}})
