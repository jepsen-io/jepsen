(defproject jepsen.influxdb "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
  					[org.clojure/clojure "1.7.0"]
  					[capacitor "0.4.3"]
  					[jepsen "0.0.8"]     
            [org.influxdb/influxdb-java "2.1"]           
  				]
 :plugins [[lein-auto "0.1.2"]]
  				)
