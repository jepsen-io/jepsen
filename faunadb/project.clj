(defproject jepsen.fauna "0.1.1"
  :description "A Jepsen test for FaunaDB"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.faunadb.runner
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [clj-yaml "0.4.0"]
                 [jepsen "0.1.14"]
                 [clj-wallhack "1.0.1"]
                 [com.faunadb/faunadb-java "2.8.0"]])
