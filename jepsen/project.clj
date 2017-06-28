(defproject jepsen "0.1.6-SNAPSHOT"
  :description "Call Me Maybe: Network Partitions in Practice"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.fressian "0.2.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [spootnik/unilog "0.7.13"]
                 [org.clojure/tools.cli "0.3.5"]
                 [clj-time "0.11.0"]
                 [knossos "0.3.1-SNAPSHOT" :exclusions [org.slf4j/slf4j-log4j12]]
                 [clj-ssh "0.5.14"]
                 [gnuplot "0.1.1"]
                 [http-kit "2.1.18"]
                 [ring "1.6.0-beta5"]
                 [hiccup "1.0.5"]
                 [org.clojars.achim/multiset "0.1.0"]
                 [byte-streams "0.2.2"]]
  :main jepsen.cli
  :plugins [[lein-localrepo "0.5.4"]]
  :aot [jepsen.cli clojure.tools.logging.impl]
;        clojure.tools.logging.impl]
  :jvm-opts ["-Xmx32g" "-XX:+UseConcMarkSweepGC" "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled" "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods" "-server"
             "-XX:-OmitStackTraceInFastThrow"])
