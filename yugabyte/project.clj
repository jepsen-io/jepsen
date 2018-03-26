(defproject yugabyte "0.1.0-SNAPSHOT"
  :description "Jepsen testing for YugaByteDB"
  :url "http://yugabyte.com/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.8"]
                 [clojurewerkz/cassaforte "2.1.0-beta1"]]
  :main yugabyte.runner
  :aot [yugabyte.runner
        clojure.tools.logging.impl])
