(defproject jepsen.dgraph "1.0.0"
  :description "Jepsen tests for Dgraph"
  :url "https://github.io/jepsen-io/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [jepsen "0.1.14-SNAPSHOT"]
                 [clj-http "3.7.0"]
                 [cheshire "5.8.0"]
                 ; Note that we specify manual versions of dgraph deps since
                 ; grpc uses version ranges
                 [io.dgraph/dgraph4j "1.6.0"
                  :exclusions [io.grpc/grpc-core
                               io.netty/netty-codec-http2]]
                 [io.grpc/grpc-core "1.10.0"]
                 [io.netty/netty-codec-http2 "4.1.16.Final"]
                 [clj-wallhack "1.0.1"]
                 [io.opencensus/opencensus-api  "0.18.0"]
                 [io.opencensus/opencensus-impl "0.18.0"]
                 [io.opencensus/opencensus-exporter-trace-jaeger "0.18.0"]]
  :main jepsen.dgraph.core
  :profiles {:uberjar {:aot :all}})
