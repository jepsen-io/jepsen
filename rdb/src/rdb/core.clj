(ns rdb.core
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [rethinkdb.core :refer [connect close]]
            [rethinkdb.query :as r]
            [knossos.core :as knossos]
            [cheshire.core :as json])
  (:import (clojure.lang ExceptionInfo)))

(defn copy-from-home [file]
    (c/scp* (str (System/getProperty "user.home") "/" file) "/root"))

(defn db []
  "Rethinkdb (ignores version)."
  (reify db/DB

    (setup! [_ test node]
      (debian/install [:libprotobuf7 :libicu48 :psmisc])
      (info node "Starting...")
      ;; TODO: detect server failing to start.
      (c/ssh* {:cmd "killall rethinkdb bash"})
      (c/ssh* {:cmd "
# Add a 100ms delay to help find edge cases.
#tc qdisc add dev eth0 root netem delay 100ms \\
#  || tc qdisc change dev eth0 root netem delay 100ms
rm -rf /root/rethinkdb_data
base=http://download.rethinkdb.com/dev
version=/2.1.0-0BETA2/rethinkdb_2.1.0%2b0BETA2~0precise_amd64.deb
if [[ ! -f /root/rdb.deb ]]; then
  curl $base/$version > /root/rdb.deb
  dpkg -i /root/rdb.deb
fi
nohup /usr/bin/rethinkdb \\
  --bind all \\
  -n `hostname` \\
  -j n1:29015 -j n2:29015 -j n3:29015 -j n4:29015 -j n5:29015 \\
  >/root/1.out 2>/root/2.out &
sleep 1 # Wait to make sure we've actually cleared out `1.out` and `2.out`.
# Wait until we see /^Server ready/ in the log file.
tail -n+0 -F /root/1.out \\
  | grep --line-buffered '^Server ready' \\
  | while read line; do pkill -P $$ tail; exit 0; done
sleep 1 # You never know.
"})
      (info node "Starting DONE!")
)

    (teardown! [_ test node]
      (info node "Tearing down db...")
      (c/ssh* {:cmd "
#tc qdisc change dev eth0 root netem delay 0ms
killall rethinkdb
f=0
# Wait for all rethinkdb instances to go away.
while [[ \"`ps auxww | grep rethinkdb | grep -v grep`\" ]] && [[ \"$f\" -lt 10 ]]; do
  sleep 1
  f=$((f+1))
done
# If it takes more than 10 seconds, murder them.
if [[ \"$f\" -ge 10 ]]; then
  killall -9 rethinkdb
fi
"})
      (info node "Tearing down db DONE!"))))

(defmacro with-errors
  "Takes an invocation operation, a set of idempotent operation
  functions which can be safely assumed to fail without altering the
  model state, and a body to evaluate. Catches RethinkDB errors and
  maps them to failure ops matching the invocation."
  [op idempotent-ops & body]
  `(let [error-type# (if (~idempotent-ops (:f ~op))
                       :fail
                       :info)]
     (try
       ~@body
       ; A server selection error means we never attempted the operation in
       ; the first place, so we know it didn't take place.
       (catch clojure.lang.ExceptionInfo e#
         (condp get (:type (ex-data e#))
           #{:op-indeterminate :unknown} (assoc ~op :type :info :error (str e#))
           (assoc ~op :type :fail :error (str e#)))))))

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  [gen]
  (gen/phases
    (->> gen
         (gen/delay 1)
         (gen/nemesis
           (gen/seq (cycle [(gen/sleep 60)
                            {:type :info :f :stop}
                            {:type :info :f :start}])))
         (gen/time-limit 600))
    ;; Recover
    (gen/nemesis
      (gen/once {:type :info :f :stop}))
    ;; Wait for resumption of normal ops
    (gen/clients
      (->> gen
           (gen/delay 1)
           (gen/time-limit 30)))))

(defn test-
  "Constructs a test with the given name prefixed by 'rdb ', merging any
  given options."
  [name opts]
  (merge
    (assoc tests/noop-test
           :name      (str "rdb " name)
           :os        debian/os
           :db        (db)
           :model     (model/cas-register)
           :checker   (checker/compose {:linear checker/linearizable})
           :nemesis   (nemesis/partition-random-halves))
    opts))
