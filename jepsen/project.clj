(defproject jepsen "0.1.12-SNAPSHOT"
  :description "Distributed systems testing framework."
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/data.fressian "0.2.1"]
                 [org.clojure/tools.logging "0.4.1"]
                 [spootnik/unilog "0.7.13"]
                 [org.clojure/tools.cli "0.4.1"]
                 [clj-time "0.11.0"]
                 [jepsen.txn "0.1.0"]
                 [knossos "0.3.4-SNAPSHOT" :exclusions [org.slf4j/slf4j-log4j12]]
                 [tea-time "1.0.1"]
                 [clj-ssh "0.5.14"]
                 [gnuplot "0.1.1"]
                 [http-kit "2.1.18"]
                 [ring "1.6.0-beta5"]
                 [hiccup "1.0.5"]
                 [org.clojars.achim/multiset "0.1.0"]
                 [byte-streams "0.2.2"]
                 [dom-top "1.0.4"]
                 [slingshot "0.12.2"]
                 [org.clojure/data.codec "0.1.1"]
                 [fipp "0.6.13"]]
  :main jepsen.cli
  :plugins [[lein-localrepo "0.5.4"]
            [lein-codox "0.10.3"]]
  :aot [jepsen.cli clojure.tools.logging.impl]
;        clojure.tools.logging.impl]
  :jvm-opts ["-Xmx32g" "-XX:+UseConcMarkSweepGC" "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled" "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods" "-server"
             ;"-XX:-OmitStackTraceInFastThrow"
             ]
  :codox {:output-path "doc/"
          :source-uri "https://github.com/jepsen-io/jepsen/blob/{version}/jepsen/{filepath}#L{line}"
          :metadata {:doc/format :markdown}})
