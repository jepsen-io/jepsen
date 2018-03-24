(defproject cockroachdb "0.1.0"
  :description "Jepsen testing for CockroachDB"
  :url "http://cockroachlabs.com/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.6"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [circleci/clj-yaml "0.5.5"]
                 [org.postgresql/postgresql "9.4.1211"]]
  :jvm-opts ~(do (require '[clojure.string :as str])
                 (into [] (concat (if (= "9" (nth (str/split (System/getProperty "java.version") #"\.") 0))
                                    ["--add-modules" "java.xml.bind"]
                                    nil)
            ["-Xmx12g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-server"])))
  :main jepsen.cockroach.runner
  :aot [jepsen.cockroach.runner
        clojure.tools.logging.impl])
