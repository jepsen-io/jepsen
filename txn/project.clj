(defproject jepsen.txn "0.1.4-SNAPSHOT"
  :description "Library for generating and analyzing multi-object transactional histories"
  :url "https://github.com/jepsen-io/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :test-selectors {:default     (fn [m] (not (or (:perf m))))
                   :all         (fn [m] true)
                   :perf        :perf}
  :dependencies [[dom-top "1.0.10"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.12.4"]
                                  [criterium "0.4.6"]]}})
