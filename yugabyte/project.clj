(defproject yugabyte "0.1.0-SNAPSHOT"
  :description "Jepsen testing for YugaByteDB"
  :url "http://yugabyte.com/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-http "3.8.0" :exclusions [commons-logging]]
                 [jepsen "0.1.8"]
                 [clojurewerkz/cassaforte "2.1.0-beta1"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]
                ]
  :main yugabyte.runner
  :aot [yugabyte.runner
        clojure.tools.logging.impl])
