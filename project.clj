(defproject jepsen.fauna "0.1.0-SNAPSHOT"
  :description "A Jepsen test for FaunaDB"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.faunadb.runner
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [clj-yaml "0.4.0"]
                 [jepsen "0.1.11-SNAPSHOT"]
                 [com.faunadb/faunadb-java "2.1.1"]
                 [com.faunadb/faunadb-java-dsl "2.1.1"]])
