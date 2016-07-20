(defproject jepsen.rethinkdb "0.1.0-SNAPSHOT"
  :description "RethinkDB Jepsen Tests"
  :url "http://github.com/rethinkdb/jepsen"
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.apa512/rethinkdb "0.12.2-SNAPSHOT"]
                 [clj-time "0.9.0"]
                 [jepsen "0.1.2"]
                 [cheshire "5.4.0"]]
  :jvm-opts ["-Xmx64g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-XX:-OmitStackTraceInFastThrow"
             "-server"])
