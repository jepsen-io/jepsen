(defproject yugabyte "0.1.2-SNAPSHOT"
  :description "Jepsen testing for YugaByteDB"
  :url "http://yugabyte.com/"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [clj-http "3.8.0" :exclusions [commons-logging]]
                 [jepsen "0.1.16"]
                 [com.yugabyte/cassaforte "3.0.0-alpha2-yb-1"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.2.5"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]
                 [clj-wallhack "1.0.1"]]
  :main yugabyte.runner)
;  :aot [yugabyte.runner
;        clojure.tools.logging.impl])
