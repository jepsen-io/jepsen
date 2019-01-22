(defproject jepsen.crate "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :main jepsen.crate.runner
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["bintray" "https://dl.bintray.com/crate/crate"]]
  :jvm-opts ["-Xmx32g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
             "-XX:-OmitStackTraceInFastThrow"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-Des.set.netty.runtime.available.processors=false"
             "-server"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.8"]
                 [cheshire "5.6.2"]
                 [org.clojure/java.jdbc "0.7.2"]
                 [clj-http "3.1.0"]
                 [org.elasticsearch.client/transport "5.6.3"]
                 [org.apache.logging.log4j/log4j-api "2.8.2"]
                 [org.apache.logging.log4j/log4j-core "2.8.2"]	
                 [io.crate/crate-jdbc "2.2.0"]])
