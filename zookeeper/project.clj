(defproject jepsen.zookeeper "0.1.1-SNAPSHOT"
  :description "Jepsen Zookeeper test"
  :url "https://github.com/aphyr/zookeeper"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [jepsen "0.0.6"]
                 [org.apache.curator/curator-recipes "2.9.0"]])
