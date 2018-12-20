(defproject jepsen.raftis "0.1.0-SNAPSHOT"
  :description "A Jepsen test for raftis"
  :url "https://github.com/Qihoo360/floyd/tree/master/floyd/example/redis"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.raftis
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.9-SNAPSHOT"]
                 [com.taoensso/carmine "2.16.0"]])
