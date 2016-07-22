(defproject cockroachdb "0.1.0-SNAPSHOT"
  :description "Jepsen testing for CockroachDB"
  :url "http://cockroachlabs.com/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [jepsen "0.1.2"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [org.postgresql/postgresql "9.4.1208"]]
  :jvm-opts ["-Xmx8g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-server"]
  :main jepsen.cockroach.runner
  :aot [jepsen.cockroach.runner
        clojure.tools.logging.impl])
