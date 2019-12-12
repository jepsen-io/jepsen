(defproject jepsen.consul "0.2.0-SNAPSHOT"
  :description "Jepsen tests for Consul"
  :url "https://github.com/aphyr/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.consul
  ;; TODO Bump clojure version latest
  ;; TODO Bump jepsen clojure version latest
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.15"]
                 [knossos "0.3.6-SNAPSHOT" :exclusions [org.slf4j/slf4j-log4j12]]
                 [base64-clj "0.1.1"]
                 [clj-http "3.10.0"]
                 [cheshire "5.9.0"]])
