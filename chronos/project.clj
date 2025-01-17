(defproject jepsen.chronos "0.1.1-SNAPSHOT"
  :description "Jepsen Mesos/Chronos test"
  :url "https://github.com/aphyr/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [jepsen.zookeeper "0.1.0"]
                 [jepsen "0.0.6"]
                 [cheshire "5.5.0"]
                 [clj-http "2.0.0"]
                 [clj-time "0.10.0"]
                 [loco "0.3.0"]])
