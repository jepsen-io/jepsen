(defproject jepsen.robustirc "0.1.0"
  :description "Jepsen tests for RobustIRC"
  :url "https://github.com/aphyr/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [jepsen "0.0.5"]
                 [clj-http "2.0.0"]
                 [digest "1.4.4"]
                 [cheshire "5.5.0"]
                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 ])
