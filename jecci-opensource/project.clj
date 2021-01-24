(defproject jecci "0.1.0-SNAPSHOT"
  :description "a common distributed system interface utilizing jepsen and elle"
  :url "not yet established"
  :license {:name "Eclipse Public License"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main jecci.common.core
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [clj-http "3.10.0"]
                 [cheshire "5.8.1"]
                 [jepsen "0.2.1"]
                 [slingshot "0.12.2"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.2.18"]
                 [mysql/mysql-connector-java "8.0.22"]
                 [org.mongodb/mongo-java-driver "3.12.7"]
                 [org.clojure/tools.logging "1.1.0"]]
  )
