(defproject jepsen.stolon "0.1.0"
  :description "Jepsen tests for Stolon, a PostgreSQL HA system."
  :url "https://github.com/jepsen-io/jepsen"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"
                  ; Also fights with aws-api
                  :exclusions [
                               org.eclipse.jetty/jetty-http
                               org.eclipse.jetty/jetty-util
                               ]]
                 [jepsen.etcd "0.2.3-SNAPSHOT"
                  ; oh god the etcd deps tree is a sprawling tire fire
                  :exclusions [
                               com.fasterxml.jackson.core/jackson-core
                               io.netty/netty-transport-native-unix-common
                               org.eclipse.jetty/jetty-http
                               org.eclipse.jetty/jetty-util
                               ]]
                 [jepsen.rds "0.1.0-SNAPSHOT"]
                 [seancorfield/next.jdbc "1.2.659"]
                 [org.postgresql/postgresql "42.7.2"]
                 [cheshire "5.12.0"]
                 [clj-wallhack "1.0.1"]]
  :main jepsen.stolon
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"]
  :repl-options {:init-ns jepsen.stolon})

