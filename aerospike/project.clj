(defproject aerospike "0.1.0"
  :description "Jepsen tests for the Aerospike database."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main aerospike.core
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.7"]
                 [io.jepsen/aerospike-client "4.1.0"]
                 [clj-wallhack "1.0.1"]])
