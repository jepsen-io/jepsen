(defproject jepsen.aerospike "0.1.0-SNAPSHOT"
  :description "Jepsen test for Disque"
  :url "https://github.com/aphyr/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [jepsen "0.0.4"]
                 [com.github.xetorthio/jedisque "0.0.4"]])
