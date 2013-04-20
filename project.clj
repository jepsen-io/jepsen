(defproject jepsen "0.0.1-SNAPSHOT"
            :description "Call Me Maybe: Network Partitions in Practice"
            :dependencies [[org.clojure/clojure "1.5.0"]
                           [aleph "0.3.0-beta16"]
                           [com.novemberain/welle "1.4.0"]
                           [com.taoensso/carmine "1.6.0"]
                           [com.novemberain/monger "1.5.0"]]
            :profiles {:dev {:dependencies [[midje "1.5.0"]]}}
            :jvm-opts ["-Xmx128m"])
