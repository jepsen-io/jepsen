(defproject jepsen "0.1.2"
  :description "Call Me Maybe: Network Partitions in Practice"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.fressian "0.2.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-time "0.11.0"]
                 [knossos "0.2.8"]
                 [clj-ssh "0.5.14"]
                 [gnuplot "0.1.1"]
                 [hiccup "1.0.5"]
                 [org.clojars.achim/multiset "0.1.0"]
                 [byte-streams "0.2.2"]]
  :jvm-opts ["-Xmx32g" "-XX:+UseConcMarkSweepGC" "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled" "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods" "-server"
             "-XX:-OmitStackTraceInFastThrow"])
