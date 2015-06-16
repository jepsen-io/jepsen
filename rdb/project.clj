(defproject mongodb "0.1.0-SNAPSHOT"
  :description "RethinkDB Jepsen Tests"
;  :url "http://github.com/rethinkdb/jepsen"
;  :license {:name "Eclipse Public License"
;            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]

                 ;; [rethinkdb "0.9.40-SNAPSHOT"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/data.json "0.2.6"]
                 [org.flatland/protobuf "0.8.1"]
                 [rethinkdb-protobuf "0.3.0"]
                 [clj-time "0.9.0"]

                 [jepsen "0.0.3"]
                 [cheshire "5.4.0"]])

