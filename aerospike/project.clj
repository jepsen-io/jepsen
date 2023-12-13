(defproject aerospike "0.1.0"
  :description "Jepsen tests for the Aerospike database."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main aerospike.core
  :dependencies [[org.clojure/clojure "1.12.0-alpha5"]
                 [jepsen "0.3.4"]
                 [com.aerospike/aerospike-client "7.2.0"]
                 [com.sun.xml.bind/jaxb-core "4.0.4"]
                 [javax.xml.bind/jaxb-api "2.4.0-b180830.0359"]
                 [com.sun.xml.bind/jaxb-impl "4.0.4"]
                 [clj-wallhack "1.0.1"]])
;;   :dependencies [
;;                  [org.clojure/clojure "1.11.1"]
;;                  [jepsen "0.1.18"]
;;                  [aerospike-client "7.2.0"]
;;                  [clj-wallhack "1.0.1"]
;;                 ;;  [:local/root "/home/nlarsen/foo/jepsen/aerospike/JavaClientRepo/aerospike-client-7.2.0.jar"]
;;                  ]
;;   :repositories {"local" ~(str (.toURI (java.io.File. "/home/nlarsen/.local/var/lein_repo/aerospike-client/aerospike-client")))}
;; )
