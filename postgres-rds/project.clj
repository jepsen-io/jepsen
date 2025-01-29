(defproject jepsen.postgres-rds "0.1.0-SNAPSHOT"
  :description "Postgres RDS tests"
  :url "http://jepsen.io"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [jepsen "0.0.7-SNAPSHOT"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [org.postgresql/postgresql "9.4-1204-jdbc42"]])
