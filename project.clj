(defproject jepsen "0.0.1-SNAPSHOT"
            :description "Call Me Maybe: Network Partitions in Practice"
            :dependencies [[org.clojure/clojure "1.5.1"]
                           [aleph "0.3.0-beta16"]
                           [clj-ssh "0.5.6"]
                           [com.novemberain/welle "1.6.0-beta2-SNAPSHOT"]
                           [com.taoensso/carmine "1.6.0"]
                           [com.novemberain/monger "1.5.0"]
                           [org.clojure/java.jdbc "0.3.0-alpha1"]
                           [korma "0.3.0-RC5"]
                           [postgresql/postgresql "8.4-702.jdbc4"]
                           [org.clojure/tools.cli "0.2.2"]
                           [com.nuodb.jdbc/nuodb-jdbc "1.1.1"]
                           [myguidingstar/clansi "1.3.0"]
                           [org.apache.curator/curator-recipes "2.0.1-incubating"]]
            :profiles {:dev {:dependencies [[midje "1.5.0"]]}}
            :main jepsen.bin
            :jvm-opts ["-Xmx512m" "-server"])
