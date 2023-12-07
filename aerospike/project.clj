(defproject aerospike "0.1.0"
  :description "Jepsen tests for the Aerospike database."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main aerospike.core
  :dependencies [
                 [org.clojure/clojure "1.11.1"]
                 [jepsen "0.1.18"]
                 [aerospike-client "7.2.0"]
                 [clj-wallhack "1.0.1"]
                ;;  [:local/root "/home/nlarsen/foo/jepsen/aerospike/JavaClientRepo/aerospike-client-7.2.0.jar"]
                 ]
  :repositories {"local" ~(str (.toURI (java.io.File. "/home/nlarsen/.local/var/lein_repo/aerospike-client/aerospike-client")))}
)
