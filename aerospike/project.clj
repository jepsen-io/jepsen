(defproject aerospike "0.1.0"
  :description "Jepsen tests for the Aerospike database."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main aerospike.core
  :aot [aerospike.core]
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.6a-aerospike"]
                 [com.aerospike/aerospike-client-jdk21-jepsen "8.1.4"]
                 [com.sun.xml.bind/jaxb-core "4.0.4"]
                 [javax.xml.bind/jaxb-api "2.4.0-b180830.0359"]
                 [com.sun.xml.bind/jaxb-impl "4.0.4"]
                 [clj-wallhack "1.0.1"]
                 ]
)
