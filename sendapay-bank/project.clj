(defproject sendapay-bank "0.1.0-SNAPSHOT"
  :description "A local Jepsen bank-style harness for Sendapay transfer confirms."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main sendapay-bank.core
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [org.clojure/tools.cli "1.3.250"]
                 [cheshire "5.13.0"]
                 [jepsen "0.3.12-SNAPSHOT"]]
  :jvm-opts ["-Xmx2g"
             "-Djava.awt.headless=true"])
