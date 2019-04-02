(defproject jepsen.zookeeper "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Zookeeper"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.zookeeper
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.13"]
                 [avout "0.5.4"]])
