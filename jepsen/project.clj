(defproject jepsen "0.0.4"
  :description "Call Me Maybe: Network Partitions in Practice"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.fressian "0.2.0"]
;                            :exclusions [org.fressian/fressian]]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-time "0.6.0"]
                 [knossos "0.2.2"]
                 [clj-ssh "0.5.11"]
                 [gnuplot "0.1.0"]
                 [hiccup "1.0.5"]
                 [org.clojars.achim/multiset "0.1.0"]
                 [org.clojure/tools.cli "0.2.2"]
                 [myguidingstar/clansi "1.3.0"]
                 [byte-streams "0.1.4"]
                 [com.netflix.curator/curator-framework "1.3.3"
                  :exclusions [org.slf4j/slf4j-api
                               org.slf4j-log4j12
                               com.google.guava/guava]]]
  :classifiers [["rabbitmq" :rabbitmq]]
  :profiles {:consul {:source-paths ["consul/src"]
                      :test-paths   ["consul/test"]
                      :dependencies [[cheshire "5.4.0"]
                                     [clj-http "1.0.1"]
                                     [base64-clj "0.1.1"]]}
             :riak {:dependencies
                    [[com.basho.riak/riak-client "1.4.4"
                      :exclusions [com.fasterxml.jackson.core/jackson-core
                                   org.apache.httpcomponents/httpclient]]]
                    :source-paths ["riak/src"]
                    :test-paths ["riak/test"]}
             :rabbitmq {:dependencies
                        [[com.novemberain/langohr "2.7.1"
                          :exclusions [com.google.guava/guava]]]
                        :source-paths ["rabbitmq/src"]
                        :test-paths ["rabbitmq/test"]}
             :etcd {:dependencies
                    [[verschlimmbesserung "0.1.1"]]
                    :source-paths ["etcd/src"]
                    :test-paths ["etcd/test"]}
             :datomic {:dependencies
                       [[com.datomic/datomic-pro "0.9.4707"
                         :exclusions [org.apache.httpcomponents/httpclient
                                      ; Why on earth is this a dep here? It
                                      ; causes circular dependencies with
                                      ; slf4j-log4j, which is ALSO a datomic
                                      ; dep via ZK...
                                      org.slf4j/log4j-over-slf4j
                                      ; Cool thanks for breaking my logging
                                      org.slf4j/slf4j-nop]]]
                       :source-paths ["datomic/src"]
                       :test-paths ["datomic/test"]}}
  :jvm-opts ["-Xmx32g" "-XX:+UseConcMarkSweepGC" "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled" "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods" "-server"])
