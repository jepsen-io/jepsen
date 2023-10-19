(defproject jepsen "0.3.5-SNAPSHOT"
  :description "Distributed systems testing framework."
  :url         "https://jepsen.io"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.fressian "1.0.0"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/tools.cli "1.0.219"]
                 [spootnik/unilog "0.7.31"]
                 [elle "0.1.8"]
                 [clj-time "0.15.2"]
                 [io.jepsen/history "0.1.2"]
                 [jepsen.txn "0.1.2"]
                 [knossos "0.3.10"]
                 [clj-ssh "0.5.14"]
                 [gnuplot "0.1.3"]
                 [http-kit "2.7.0"]
                 [ring "1.10.0"]
                 [com.hierynomus/sshj "0.37.0"]
                 [com.jcraft/jsch.agentproxy.connector-factory "0.0.9"]
                 [com.jcraft/jsch.agentproxy.sshj "0.0.9"
                  :exclusions [net.schmizz/sshj]]
                 [org.bouncycastle/bcprov-jdk15on "1.70"]
                 [hiccup "1.0.5"]
                 [metametadata/multiset "0.1.1"]
                 [byte-streams "0.2.5-alpha2"]
                 [slingshot "0.12.2"]
                 [org.clojure/data.codec "0.1.1"]
                 [fipp "0.6.26"]]
  :java-source-paths ["src"]
  :javac-options ["-target" "11" "-source" "11"]
  :main jepsen.cli
  :plugins [[lein-localrepo "0.5.4"]
            [lein-codox "0.10.8"]
            [jonase/eastwood "0.3.10"]]
  :jvm-opts ["-Xmx32g"
             "-Djava.awt.headless=true"
             "-server"]
  :test-selectors {:default (fn [m]
                              (not (or (:perf m)
                                       (:integration m)
                                       (:logging m))))
                   :focus       :focus
                   :perf        :perf
                   :logging     :logging
                   :integration :integration}
  :codox {:output-path "doc/"
          :source-uri "https://github.com/jepsen-io/jepsen/blob/v{version}/jepsen/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :profiles {:uberjar {:aot :all}
             :dev {; experimenting with faster startup
                   ;:aot [jepsen.core]
                   :dependencies [[org.clojure/test.check "1.1.1"]]
                   :jvm-opts ["-Xmx32g"
                              "-server"
                              "-XX:-OmitStackTraceInFastThrow"]}})
