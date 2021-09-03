(defproject jepsen.consul "0.2.0-SNAPSHOT"
  :description "Jepsen tests for Consul"
  :url "https://github.com/aphyr/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.16"]
                 [base64-clj "0.1.1"]
                 [clj-http "3.10.0"]
                 [cheshire "5.9.0"]]
  :repl-options {:init-ns jepsen.consul}
  :main jepsen.consul)
