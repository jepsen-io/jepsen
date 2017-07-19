(defproject tidb "0.1.0-SNAPSHOT"
  :description "Jepsen testing for TiDB"
  :url "http://pingcap.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main tidb.core
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.6"]
                ]
)
