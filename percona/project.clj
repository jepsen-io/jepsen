(defproject jepsen.percona "0.1.0-SNAPSHOT"
  :description "PerconaDB tests"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [jepsen "0.0.7-SNAPSHOT"]
                 [jepsen.galera "0.1.0-SNAPSHOT"]
                 [honeysql "0.6.1"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [org.mariadb.jdbc/mariadb-java-client "1.2.0"]])
