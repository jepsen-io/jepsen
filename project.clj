(defproject jepsen "0.0.3-SNAPSHOT"
            :description "Call Me Maybe: Network Partitions in Practice"
            :dependencies [[org.clojure/clojure "1.6.0-beta1"]
                           [aleph "0.3.0-beta16"]
                           [knossos "0.1.1-SNAPSHOT"]
                           [clj-ssh "0.5.7"]
                           [org.clojars.achim/multiset "0.1.0-SNAPSHOT"]
                           [com.novemberain/welle "2.0.0-beta1"]
                           [com.taoensso/carmine "1.6.0"]
                           [com.novemberain/monger "1.5.0"]
                           [org.clojure/java.jdbc "0.3.0-alpha1"]
                           [korma "0.3.0-RC5"]
                           [postgresql/postgresql "8.4-702.jdbc4"]
                           [org.clojure/tools.cli "0.2.2"]
                           [com.nuodb.jdbc/nuodb-jdbc "1.1.1"]
                           [myguidingstar/clansi "1.3.0"]
                           [org.apache.curator/curator-recipes "2.0.1-incubating"
                            :exclusions [org.jboss.netty/netty]]
                           [clj-kafka "0.1.2-0.8"]
                           [clojurewerkz/cassaforte "1.2.0"
                            :exclusions [com.datastax.cassandra/cassandra-driver-core]]
                           [com.datastax.cassandra/cassandra-driver-core "1.0.3"]
                           [byte-streams "0.1.4"]
                           [org.clojure/math.combinatorics "0.0.4"]
                           [com.novemberain/langohr "2.7.1"]
                           [com.foundationdb/fdb-java "2.0.0"]]
            :profiles {:dev {:dependencies [[midje "1.5.0"]]}}
            :main jepsen.bin
            :jvm-opts ["-Xmx32g" "-XX:+UseConcMarkSweepGC" "-XX:+UseParNewGC"
                       "-XX:+CMSParallelRemarkEnabled" "-XX:+AggressiveOpts"
                       "-XX:+UseFastAccessorMethods" "-server"])
