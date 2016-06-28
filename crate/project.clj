(defproject jepsen.crate "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["bintray" "http://dl.bintray.com/crate/crate"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.2-SNAPSHOT"]
                 [cheshire "5.6.2"]
                 [io.crate/crate-client "0.54.9"]])
