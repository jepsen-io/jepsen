(defproject tidb "0.1.0-SNAPSHOT"
  :description "Jepsen testing for TiDB"
  :url "https://pingcap.com/index"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main tidb.core
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.14-SNAPSHOT"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [org.mariadb.jdbc/mariadb-java-client "1.2.0"]]
  :aot [tidb.core clojure.tools.logging.impl])
