(ns jepsen.role
  "Supports tests where each node has a single, distinct role. For instance,
  one node might run ZooKeeper, and the remaining nodes might run Kafka.

  Using this namespace requires the test to have a :roles map, whose keys are
  arbitrary roles, and whose corresponding values are vectors of nodes in the
  test, like so:

     {:mongod [\"n1\" \"n2\"]
      :mongos [\"n3\"]}"
  (:require [dom-top.core :refer [loopr]]
            [jepsen.db :as db]
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
  [test role]
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
         ~'test (restrict-test ~'test ~'role)]
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
                         test (restrict-test test role)]
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
                       test (restrict-test test role)]
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
