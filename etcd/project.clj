(defproject jepsen.etcd "0.1.0-SNAPSHOT"
  :description "A Jepsen test for etcd"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.etcd
  :jvm-opts ["-Dcom.sun.management.jmxremote"]
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.13-SNAPSHOT"]
                 [verschlimmbesserung "0.1.3"]])
