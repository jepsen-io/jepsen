(defproject jepsen.dgraph "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [jepsen "0.1.8"]
                 [clj-http "3.7.0"]
                 [cheshire "5.8.0"]
                 ; Note that we specify manual versions of dgraph deps since
                 ; grpc uses version ranges
                 [io.dgraph/dgraph4j "1.2.0"
                  :exclusions [io.grpc/grpc-core
                               io.netty/netty-codec-http2]]
                 [io.grpc/grpc-core "1.7.0"]
                 [io.netty/netty-codec-http2 "4.1.16.Final"]
                 [clj-wallhack "1.0.1"]]
  :main jepsen.dgraph.core)
