(ns jepsen.db.watchdog
  "Databases often like to crash, and they may not restart themselves
  automatically,  which means we have to do it for them. This creates the
  possibility of all kinds of race conditions. This namespace provides a
  watchdog which runs in a thread for the duration of the test, and a `DB`
  wrapper for unreliable DBs.

  Watchdogs know whether the server they supervise is *running*. They can be
  *enabled* or *disabled*, which determines whether they restart the server or
  not. They can be *killed* once, which destroys their thread. They can also be
  locked, during which the watchdog takes no actions."
  (:refer-clojure :exclude [run! locking])
  (:require [clojure [core :as clj]]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util :refer [timeout]]]
            [dom-top.core :refer [with-retry]]
            [potemkin :refer [definterface+]]))

(definterface+ IWatchdog
  ; Public API
  (kill! [w] "Kills the watchdog, terminating its thread. Blocks until complete.")

  (enable! [w] "Informs the watchdog that it should restart the daemon. Returns immediately.")

  (disable! [w] "Informs the watchdog that it should not restart the daemon. Returns once we can guarantee the watchdog won't restart.")

  ; Internal API
  (step! [w test node] "The main step of the watchdog. Periodically invoked by the runner.")

  (run! [w test node] "Starts the mainloop, calling step! repeatedly. Returns watchdog immediately."))

(defrecord Watchdog
  [; How long, in ms, between steps
   ^long interval
   ; Function which tells us if the server is running
   running?
   ; Function which starts the server
   start!
   ; An atom: are we enabled?
   enabled?
   ; A promise, delivered when we exit
   killed
   ; A promise of a future--our worker thread
   fut]

  IWatchdog
  (kill! [_]
    (if-let [f (deref fut 0 nil)]
      (do (future-cancel f)
          @killed)
      ; You can kill a watchdog before it starts; that's a no-op. This
      ; simplifies teardown! code, which is run both before and after setup.
      :not-running))

  (enable! [this]
    (clj/locking this
      (reset! enabled? true)))

  (disable! [this]
    ; Acquiring a lock here means we never race with step!
    (clj/locking this
      (reset! enabled? false)))

  (step! [this test node]
    (util/with-thread-name (str "jepsen watchdog " node)
      (clj/locking this
        (when @enabled?
          (let [running?
                (timeout interval
                         (do (warn "Watchdog's `running?` function"
                                   running? "timed out after"
                                   interval "ms.")
                             false)
                         (running? test node))]
            (debug node "is" (if running? "running" "dead"))
            (when-not running?
              (let [r (start! test node)]
                (info "Watchdog started:" r))))))))

  (run! [this test node]
    (when (realized? fut)
      (throw (IllegalStateException. "Can't run a watchdog twice!")))
    ; A little awkward, but this future basically exists so we can spawn
    ; threads with on-nodes and return immediately. It blocks indefinitely, but
    ; chances are you'll only have a handful of nodes to watchdog, so NBD.
    (deliver
      fut
      (future
        (util/with-thread-name (str "jepsen watchdog supervisor " node)
          (with-retry []
            (Thread/sleep interval)
            (c/on-nodes test [node] (partial step! this))
            (retry)
            (catch InterruptedException e
              (deliver killed :killed)
              (throw e))
            (catch RuntimeException t
              (warn t "Unexpected error in watchdog worker")
              (retry))
            (catch Throwable t
              ; Don't retry; this might be an OOM etc.
              (deliver killed :crashed)
              (warn t "Unexpected fatal error in watchdog worker"))))))
    this))

(defn watchdog
  "Creates a new Watchdog for a single node. Takes an options map with:

      {:running?  A function (running? test node) which returns true iff the
                  server is running.
       :start!    A function (start! test node) which starts the server.
       :interval  Time, in ms, between checking to restart the server.
                  Default 1000.}

  Both running? and start? are evaluated with a jepsen.control connection bound
  to the given node."
  [{:keys [running? start!] :as opts}]
  (assert (fn? running?))
  (assert (fn? start!))
  (let [interval (:interval opts 1000)]
    (map->Watchdog
      {:interval (long interval)
       :running? running?
       :start!   start!
       :enabled? (atom true)
       :killed   (promise)
       :fut      (promise)})))

(defmacro locking
  "Locks a Watchdog for the duration of body. Use this around any of your code
  that starts/stops the server, to avoid race conditions where you e.g. kill
  the server and the watchdog immediately restarts it."
  [watchdog & body]
  `(clj/locking watchdog ~@body))

(defrecord DB
  [db         ; Wrapped database
   opts       ; Options map for spawning watchdogs
   watchdogs] ; Atom: a map of node->watchdog
  db/DB
  (setup! [this test node]
    (db/setup! db test node)
    (let [w (watchdog opts)]
      (swap! watchdogs assoc node w)
      (run! w test node)))

  (teardown! [this test node]
    (when-let [w (get @watchdogs node)]
      (kill! w)
      (db/teardown! db test node)))

  db/Kill
  (kill! [_ test node]
    (if-let [w (get @watchdogs node)]
      (locking w
        (disable! w)
        (db/kill! db test node))
      (db/kill! db test node)))

  (start! [_ test node]
    (if-let [w (get @watchdogs node)]
      (locking w
        (db/start! db test node)
        (enable! w))
      (db/start! db test node)))

  db/Pause
  (pause! [_ test node] (db/pause! db test node))
  (resume! [_ test node] (db/resume! db test node))

  db/LogFiles
  (log-files [_ test node] (db/log-files db test node))

  db/Primary
  (primaries [_ test] (db/primaries db test))
  (setup-primary! [_ test node] (db/setup-primary! db test node)))

(defn db
  "Wraps an existing database in a one with watchdogs for each node. Takes a
  map of partial options to `watchdog` (just :running? and :interval), and a DB
  to wrap. Uses `db/start!` to start the database. Ensures that db/kill! and
  db/start! disable and enable the watchdog.

  The DB you wrap must implement the full suite of DB protocols--LogFiles,
  Primary, etc. This is a little awkward, but feels preferable to having a
  zillion variants of the DB class here. You can generally return `nil` from
  (e.g.) log-files, primaries, etc., and Jepsen will do sensible things."
  [opts db]
  (map->DB
    {:db        db
     :watchdogs (atom {})
     :opts      (assoc opts :start! (partial db/start! db))}))
