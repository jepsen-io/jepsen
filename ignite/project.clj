(defproject jepsen.ignite "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Apache Ignite"
  :url "https://ignite.apache.org/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.19"]
                 [org.apache.ignite/ignite-core "2.8.0"]
                 [org.apache.ignite/ignite-spring "2.8.0"]
                 [org.apache.ignite/ignite-log4j "2.8.0"]]
  :java-source-paths ["src/java"]
  :target-path "target/%s"
  :main jepsen.ignite.runner
  :aot [jepsen.ignite.runner])
