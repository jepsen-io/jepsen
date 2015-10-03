(defproject jepsen.zookeeper.leaderd "0.1.0-SNAPSHOT"
  :description "Little ZK leader election demo"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.zookeeper.leaderd
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [aleph "0.4.0"]
                 [cheshire "5.5.0"]
                 [org.apache.curator/curator-recipes "2.9.0"]])
