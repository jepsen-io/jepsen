(defproject jepsen "0.2.1"
  :description "Distributed systems testing framework."
  :url         "https://jepsen.io"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.fressian "1.0.0"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.clojure/tools.cli "1.0.194"]
                 [spootnik/unilog "0.7.25"]
                 [elle "0.1.2"]
                 [clj-time "0.15.2"]
                 [jepsen.txn "0.1.2"]
                 [knossos "0.3.7"]
                 [tea-time "1.0.1"]
                 [clj-ssh "0.5.14"]
                 [gnuplot "0.1.3"]
                 [http-kit "2.3.0"]
                 [ring "1.8.1"]
                 [hiccup "1.0.5"]
                 [metametadata/multiset "0.1.1"]
                 [byte-streams "0.2.5-alpha2"]
                 [dom-top "1.0.5"]
                 [slingshot "0.12.2"]
                 [org.clojure/data.codec "0.1.1"]
                 [fipp "0.6.23"]
                 [io.lacuna/bifurcan "0.1.0"]]
  :main jepsen.cli
  :plugins [[lein-localrepo "0.5.4"]
            [lein-codox "0.10.7"]
            [jonase/eastwood "0.3.10"]]
  :jvm-opts ["-Xmx32g"
             "-Djava.awt.headless=true"
             "-server"]
  :test-selectors {:default (fn [m]
                              (not (or (:perf m)
                                       (:integration m)
                                       (:logging m))))
                   :perf        :perf
                   :logging     :logging
                   :integration :integration}
  :codox {:output-path "doc/"
          :source-uri "https://github.com/jepsen-io/jepsen/blob/{version}/jepsen/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :profiles {:uberjar {:aot :all}
             :dev {:jvm-opts ["-Xmx32g"
                              "-server"
                              "-XX:-OmitStackTraceInFastThrow"]}})
