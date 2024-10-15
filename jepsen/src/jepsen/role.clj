(ns jepsen.role
  "Supports tests where each node has a single, distinct role. For instance,
  one node might run ZooKeeper, and the remaining nodes might run Kafka.

  Using this namespace requires the test to have a :roles map, whose keys are
  arbitrary roles, and whose corresponding values are vectors of nodes in the
  test, like so:

     {:mongod [\"n1\" \"n2\"]
      :mongos [\"n3\"]}"
  (:require [dom-top.core :refer [loopr]]
            [jepsen [client :as client]
                    [db :as db]
                    [nemesis :as n]]
            [jepsen.nemesis.combined :as nc]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent CyclicBarrier)))

(defn role
  "Takes a test and node. Returns the role for that particular node. Throws if
  the test does not define a role for that node."
  [test node]
  (loopr []
         [[role nodes] (:roles test)
          n nodes]
         (if (= n node)
           role
           (recur))
         (throw+ {:type ::no-role-for-node
                  :node node
                  :roles (:roles test)})))

(defn nodes
  "Returns a vector of nodes associated with a given role, in test order. Right
  now this returns nil when given a role not in the roles map. I'm not sure if
  that's more or less useful than throwing. This may change."
  [test role]
  (get (:roles test) role))

(defn restrict-test
  "Takes a test map and a role. Returns a version of the test map where the
  :nodes are only those for this specific role, and the :barrier is replaced by
  a fresh CyclicBarrier for the appropriate number of nodes."
  [role test]
  (let [nodes (nodes test role)
        n     (count nodes)]
    (assoc test
           :nodes       nodes
           :barrier     (if (pos? n)
                          (CyclicBarrier. n)
                          :jepsen.core/no-barrier))))

(defmacro db-helper*
  "We have to figure out the role, db, and restricted test for every single fn.
  This anaphoric macro binds these variables to strip out boilerplate. Only for
  use in DB below, or when writing your own DB and you use *exactly* the form
  below."
  [& body]
  `(let [~'role (role ~'test ~'node)
         ~'db   (or (get ~'dbs ~'role)
                    (throw+ {:type ::no-db-for-role
                             :role ~'role
                             :dbs  ~'dbs}))
         ~'test (restrict-test ~'role ~'test)]
     ~@body))

(defrecord DB
  [dbs]
  db/DB
  (setup! [_ test node] (db-helper* (db/setup! db test node)))
  (teardown! [_ test node] (db-helper* (db/teardown! db test node)))

  db/Kill
  (kill! [_ test node] (db-helper* (db/kill! db test node)))
  (start! [_ test node] (db-helper* (db/start! db test node)))

  db/Pause
  (pause!  [_ test node] (db-helper* (db/pause! db test node)))
  (resume! [_ test node] (db-helper* (db/resume! db test node)))

  db/Primary
  (primaries [db test]
    ; Call for each role, then combine
    (->> (:roles test)
         keys
         (mapcat (fn [role]
                   (let [db   (get dbs role)
                         test (restrict-test role test)]
                     (when (satisfies? db/Primary db)
                       (db/primaries db test)))))
         (into [])))

  ; Setup-primary! always uses the first node; we do that for each role
  ; independently iff they support Primary.
  (setup-primary! [db test node]
    (->> (:roles test)
         keys
         (mapv (fn [role]
                 (let [db   (get dbs role)
                       test (restrict-test role test)]
                   (when (satisfies? db/Primary db)
                     (db/setup-primary! db test (first (:nodes test)))))))))


  db/LogFiles
  (log-files [db test node]
    (db-helper* (db/log-files db test node))))

(defn db
  "Takes a map of role -> DB and creates a composite DB which implements the
  full suite of DB protocols. Setup! on this DB calls the setup! for that
  particular role's DB for that node, with a restricted test, and so
  on."
  [dbs]
  (DB. dbs))

(defrecord RestrictedClient [role client]
  client/Client
  (open! [this test node]
    (let [node-index  (.indexOf ^java.util.List (:nodes test) node)
          role-nodes  (nodes test role)
          _           (assert (pos? (count role-nodes))
                              (str "No nodes for role " (pr-str role)
                                   " (roles are " (pr-str (:roles test))))
          node        (nth role-nodes (mod node-index (count role-nodes)))]
      (client/open! client test node))))

(defn restrict-client
  "Your test has nodes with different roles, but only one role is client-facing.

  Wraps a jepsen Client `c` in a new Client specific to the given role. This
  client responds *only* to (client/open! wrapper test node). Instead of
  connecting to the given node, calls `(client/open! c test node'), where node'
  is a node with the given role.

  Note that this wrapper evaporates after open!; the inner client takes over
  thereafter. Calls to `invoke!` etc go directly to the inner client and will
  receive the full test map, rather than a restricted one. This may come back
  to bite us later, in which case we'll change."
  [role client]
  (RestrictedClient. role client))

(defrecord RestrictedNemesis [role nemesis]
  n/Reflection
  (fs [_] (n/fs nemesis))

  n/Nemesis
  (setup! [this test]
    (RestrictedNemesis. role (n/setup! nemesis (restrict-test role test))))

  (invoke! [this test op]
    (n/invoke! nemesis (restrict-test role test) op))

  (teardown! [this test]
    (n/teardown! nemesis (restrict-test role test))))

(defn restrict-nemesis
  "Wraps a Nemesis in a new one restricted to a specific role. Calls to the
  underlying nemesis receive a restricted test map."
  [role nemesis]
  (RestrictedNemesis. role nemesis))

(defn restrict-nemesis-package
  "Restricts a jepsen.nemesis.combined package to act purely on a single role.
  Right now we just restrict the nemesis, not the generators; maybe later we'll
  need to do the generators too. Operations in this package have their
  operation `:f`s lifted to `:f [role f]`. Also adds a :role key to the
  package, for your use later."
  [role package]
  (-> package
      (assoc :role    role
             :nemesis (restrict-nemesis role (:nemesis package)))
      (->> (nc/f-map (partial vector role)))))
