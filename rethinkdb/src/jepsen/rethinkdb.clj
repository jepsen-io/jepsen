(ns jepsen.rethinkdb
  (:refer-clojure :exclude [run!])
  (:require [clojure [pprint :refer :all]
                     [string :as str]
                     [set    :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout retry with-retry]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [net       :as net]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [rethinkdb.core :refer [connect close]]
            [rethinkdb.query :as r]
            [rethinkdb.query-builder :refer [term]]
            [knossos.core :as knossos]
            [knossos.model :as model]
            [cheshire.core :as json])
  (:import (clojure.lang ExceptionInfo)))

(def log-file "/var/log/rethinkdb")

(defn faketime-script
  "A sh script which invokes cmd with a faketime wrapper."
  [cmd]
  (str "#!/bin/bash\n"
       "faketime -m -f \"+$((RANDOM%100))s x1.${RANDOM}\" "
       cmd
       " \"$@\""))

(defn faketime-wrapper!
  "Replaces an executable with a faketime wrapper. Idempotent."
  [cmd]
  (let [cmd'    (str cmd ".no-faketime")
        wrapper (faketime-script cmd')]
    (when-not (cu/exists? cmd')
      (info "Installing faketime wrapper.")
      (c/exec :mv cmd cmd')
      (c/exec :echo wrapper :> cmd)
      (c/exec :chmod "a+x" cmd))))

(defn install!
  "Install RethinkDB on a node"
  [node version]
  ; Install package
  (debian/add-repo! "rethinkdb"
                    "deb http://download.rethinkdb.com/apt jessie main")
  (c/su (c/exec :wget :-qO :- "https://download.rethinkdb.com/apt/pubkey.gpg" |
                :apt-key :add :-))
  (debian/install {"rethinkdb" version})
  (faketime-wrapper! "/usr/bin/rethinkdb")

  ; Set up logfile
  (c/exec :touch log-file)
  (c/exec :chown "rethinkdb:rethinkdb" log-file))

(defn join-lines
  "A string of config file lines for nodes to join the cluster"
  [test]
  (->> test
       :nodes
       (map (fn [node] (str "join=" (name node) :29015)))
       (clojure.string/join "\n")))

(defn configure!
  "Set up configuration files"
  [test node]
  (info "Configuring" node)
  (c/su
    (c/exec :echo (-> "jepsen.conf"
                      io/resource
                      slurp
                      (str "\n\n"
                           (join-lines test) "\n\n"
                           "server-name=" (name node) "\n"
                           "server-tag=" (name node) "\n"))
            :> "/etc/rethinkdb/instances.d/jepsen.conf")))

(defn start!
  "Starts the rethinkdb service"
  [node]
  (c/su
    (info node "Starting rethinkdb")
    (c/exec :service :rethinkdb :start)
    (info node "Started rethinkdb")))

(defn conn
  "Open a connection to the given node."
  [node]
  (connect :host (name node) :port 28015))

(defn wait-for-conn
  "Wait until a connection can be opened to the given node."
  [node]
  (info "Waiting for connection to" node)
  (retry 5 (close (conn node)))
  (info node "ready"))

(defn run!
  "Like rethinkdb.query/run, but asserts that there were no errors."
  [query conn]
  (let [result (r/run query conn)]
    (when (contains? result :errors)
      (assert (zero? (:errors result)) (:first_error result)))
    result))

(defn wait-table
  "Wait for all replicas for a table to be ready"
  [conn db tbl]
  (run! (term :WAIT [(r/table (r/db db) tbl)] {}) conn))

(defn db
  "Set up and tear down RethinkDB"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (install! node version)
      (configure! test node)
      (start! node)

      (wait-for-conn node))

    (teardown! [_ test node]
      (info node "Nuking" node "RethinkDB")
      (cu/grepkill! "rethinkdb")
      (c/su
        (c/exec :rm :-rf "/var/lib/rethinkdb/jepsen")
        (c/exec :truncate :-c :--size 0 log-file))
      (info node "RethinkDB dead"))

    db/LogFiles
    (log-files [_ test node] [log-file])))

(defmacro with-errors
  "Takes an invocation operation, a set of idempotent operation
  functions which can be safely assumed to fail without altering the
  model state, and a body to evaluate. Catches RethinkDB errors and
  maps them to failure ops matching the invocation.

  Also includes an automatic timeout of 5000 ms."
  [op idempotent-ops & body]
  `(let [error-type# (if (~idempotent-ops (:f ~op))
                       :fail
                       :info)]
     (timeout 5000 (assoc ~op :type error-type# :error :timeout)
              (try
                ~@body
                (catch clojure.lang.ExceptionInfo e#
                  (let [code# (-> e# ex-data :response :e)]
                    (assert (integer? code#))
                    (case code#
                      4100000 (assoc ~op :type :fail,       :error (:cause (ex-data e#)))
                      (assoc ~op :type error-type#, :error (str e#)))))))))

(defn primaries
  "All nodes that think they're primaries for the given db and table"
  [nodes db table]
  (->> nodes
       (pmap (fn [node]
               (-> (r/db db)
                   (r/table table)
                   (r/status)
                   (run! (conn node))
                   :shards
                   (->> (mapcat :primary_replicas)
                        (some #{(name node)}))
                   (when node))))
       (remove nil?)))

(defn reconfigure!
  "Reconfigures replicas for a table."
  [conn db table primary replicas]
  (let [res (-> (r/db db)
                (r/table table)
                (r/reconfigure {:shards   1
                                :replicas (->> replicas
                                               (map name)
                                               (map #(vector % 1))
                                               (into {}))
                                :primary_replica_tag (name primary)})
                (run! conn))]
    (assert (= 1 (:reconfigured res)))
    (info "reconfigured" {:replicas replicas, :primary primary})
    res))

(defn reconfigure-nemesis
  "A nemesis which randomly reconfigures the cluster topology for the given db
  and table names."
  [db table]
  (reify client/Client
    (setup! [this _ _] this)

    (invoke! [_ test op]
      (assert (= :reconfigure (:f op)))
      (timeout 5000 (assoc op :value :timeout)
         (with-retry [i 10]
           (let [size     (inc (rand-int (count (:nodes test))))
                 replicas (->> (:nodes test)
                               shuffle
                               (take size))
                 primary  (rand-nth replicas)
                 conn     (conn primary)]
             (try
               (info "will reconfigure to" replicas "(" primary ")")
               (reconfigure! conn db table primary replicas)
               (assoc op :value {:replicas replicas :primary primary})
               (finally (close conn))))
           (catch clojure.lang.ExceptionInfo e
             (if (zero? i)
               (throw e)
               (condp re-find (.getMessage e)
                 #"Could not find any servers with server tag"
                 (do (warn "reconfigure caught; retrying:" (.getMessage e))
                     (retry (dec i)))

                 #"The server\(s\) hosting table .+? are currently unreachable."
                 (do (warn "reconfigure failed: servers unreachable. retrying")
                     (retry (dec i)))

                 (throw e)))))))

    (teardown! [_ test])))

(defn reconfigure-grudge
  "Computes a network partition grudge likely to mess up the given primary and
  replicas."
  [nodes primary replicas primary' replicas']
  (let [; Construct a grudge which splits the cluster in half, each
        ; primary in a different side.
        component1 (->> (disj nodes primary')
                        shuffle
                        (take (/ (count nodes) 2))
                        set)
        component2 (set/difference nodes component1)]
    (nemesis/complete-grudge [component1 component2])
    ; Disregard that, pick randomly
    (if (< (rand) 0.5)
      {}
      (nemesis/complete-grudge (nemesis/bisect (shuffle nodes))))))

(defn aggressive-reconfigure-nemesis
  "A nemesis which reconfigures the cluster topology for the given db
  and table names, introducing partitions selected particularly to break
  guarantees."
  ([db table]
   (aggressive-reconfigure-nemesis db table (atom {})))
  ([db table state]
   (reify client/Client
     (setup! [this _ _] this)

     (invoke! [_ test op]
       (assert (= :reconfigure (:f op)))
       (locking state
         (timeout 10000 (assoc op :value :timeout)
           (with-retry [i 10]
             (let [{:keys [replicas primary grudge]
                    :or   {primary  (first (:nodes test))
                           replicas [(first (:nodes test))]
                           grudge   {}}}                    @state
                   nodes   (set (:nodes test))
                   ; Pick a new primary and replicas at random.
                   size' (inc (rand-int (count nodes)))
                   replicas' (->> nodes
                                 shuffle
                                 (take size'))
                   primary' (rand-nth replicas')

                   ; Pick a new primary visible to the current one.
                   ;primary' (-> nodes
                   ;             (disj primary)
                   ;             (set/difference (set (grudge primary)))
                   ;             vec
                   ;             rand-nth)
                   ; Pick a new set of replicas including the primary, visible
                   ; to the new primary.
                   ;size'     (rand-int (count nodes))
                   ;replicas' (->> (-> nodes
                   ;                   (disj primary')
                   ;                   (set/difference (set (grudge primary'))))
                   ;              shuffle
                   ;              (take size')
                   ;              (cons primary'))

                   grudge'   (reconfigure-grudge nodes primary  replicas
                                                       primary' replicas')
                   conn      (conn primary')]
               (try
                 ; Reconfigure
                 (info "will reconfigure" primary replicas "->" primary' replicas' " under grudge" grudge)
                 (reconfigure! conn db table primary' replicas')

                 ; Set up new network topology
                 (net/heal! (:net test) test)
                 (info "network healed")
                 (nemesis/partition! test grudge')
                 (info "network partitioned:" grudge')

                 ; Save state for next time
                 (reset! state {:primary primary'
                                :replicas replicas'
                                :grudge  grudge'})

                 ; Return
                 (assoc op :value @state)
                 (finally (close conn))))
           (catch clojure.lang.ExceptionInfo e
             (if (zero? i)
               (throw e)
               (condp re-find (.getMessage e)
                 #"Could not find any servers with server tag"
                 (do (warn "reconfigure caught; retrying:" (.getMessage e))
                     (retry (dec i)))

                 #"The server\(s\) hosting table .+? are currently unreachable."
                 (do (warn "reconfigure failed: servers unreachable. Healing net and retrying")
                     (net/heal! (:net test) test)
                     (retry (dec i)))

                 (throw e))))))))

     (teardown! [_ test]))))

(defn test-
  "Constructs a test with the given name prefixed by 'rethinkdb ', merging any
  given options."
  [name opts]
  (merge
    (assoc tests/noop-test
           :name      (str "rethinkdb " name)
           :os        debian/os
           :db        (db (:version opts))
           :model     (model/cas-register)
           :checker   (checker/perf))
    (dissoc opts :version)))
