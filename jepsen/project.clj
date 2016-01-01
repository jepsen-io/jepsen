(defproject jepsen "0.0.9"
  :description "Call Me Maybe: Network Partitions in Practice"
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.fressian "0.2.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-time "0.6.0"]
                 [knossos "0.2.4"]
                 [clj-ssh "0.5.11"]
                 [gnuplot "0.1.1"]
                 [hiccup "1.0.5"]
                 [org.clojars.achim/multiset "0.1.0"]
                 [byte-streams "0.1.4"]]
  :jvm-opts ["-Xmx32g" "-XX:+UseConcMarkSweepGC" "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled" "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods" "-server"])
