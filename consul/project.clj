(defproject jepsen.consul "0.1.0"
  :description "Jepsen tests for Consul"
  :url "https://github.com/aphyr/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [jepsen "0.0.5"]
                 [cheshire "5.4.0"]
                 [clj-http "1.0.1"]
                 [base64-clj "0.1.1"]])
