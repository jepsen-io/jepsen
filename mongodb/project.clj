(defproject jepsen.mongodb "0.2.0-SNAPSHOT"
  :description "Jepsen MongoDB tests"
  :url "https://github.com/aphyr/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.cli "0.3.3"]
                 [jepsen "0.1.0"]
                 [cheshire "5.4.0"]
                 [org.mongodb/mongodb-driver "3.2.2"]]
  :main jepsen.mongodb.runner
  :aot [jepsen.mongodb.runner
        clojure.tools.logging.impl])
