(ns jepsen.db
  "Allows Jepsen to set up and tear down databases."
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+]]
            [jepsen [control :as control]
                    [util :as util :refer [fcatch meh]]]
            [jepsen.control [util :as cu]
                            [net :as cn]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defprotocol DB
  (setup!     [db test node] "Set up the database on this particular node.")
  (teardown!  [db test node] "Tear down the database on this particular node."))

(defprotocol Kill
  "This optional protocol supports starting and killing a DB's processes."
  (kill!  [db test node] "Forcibly kills the process")
  (start! [db test node] "Starts the process"))

; Process is imported by default from java.lang. The eval here is an attempt to
; keep lein install from generating broken... cached... something or other. I
; can't figure out why this breaks.
(eval
  '(do (ns-unmap 'jepsen.db 'Process)
       ; It's going to jump ahead and compile this def ... ahead of the
       ; ns-unmap, I think? So we wrap it in a *second* eval. JFC, what a hack.
       (eval '(def Process Kill))))

(defprotocol Pause
  "This optional protocol supports pausing and resuming a DB's processes."
  (pause!   [db test node] "Pauses the process")
  (resume!  [db test node] "Resumes the process"))

(defprotocol Primary
  "This optional protocol supports databases which have a notion of one (or
  more) primary nodes."
  (primaries [db test]
             "Returns a collection of nodes which are currently primaries.
             Best-effort is OK; in practice, this usually devolves to 'nodes
             that think they're currently primaries'.")
  (setup-primary! [db test node] "Performs one-time setup on a single node."))

(defprotocol LogFiles
  (log-files [db test node]
             "Returns either a.) a map of fully-qualified remote paths (on this
             DB node) to short local paths (in store/), or b.) a sequence of
             fully-qualified remote paths."))

(defn log-files-map
  "Takes a DB, a test, and a node. Returns a map of remote paths to local
  paths. Checks to make sure there are no duplicate local paths.

  log-files used to return a sequence of remote paths, and some people are
  likely still assuming that form for composition. When they start e.g.
  concatenating maps into lists of strings, we're going to get mixed
  representations. We try to make all this Just Work (TM)."
  [db test node]
  (let [log-files (log-files db test node)
        log-files (if (map? log-files)
                    ; We're done here; the DB knows exactly what short names
                    ; they want.
                    log-files
                    ; OK, we've got a sequence. Break that up into two
                    ; parts--those where we know the short name (kv pairs), and
                    ; those where we only know the remote long name (strings).
                    (let [short-files (into {} (filter map-entry? log-files))
                          long-files  (remove map-entry? log-files)
                          ; Now try to figure out short names for the long files
                          auto-shorts (->> long-files
                                           (map #(str/split % #"/"))
                                           util/drop-common-proper-prefix
                                           (map (partial str/join "/"))
                                           (zipmap log-files))]
                      (merge short-files auto-shorts)))]
    ; Check for collisions in short names
    (assert+ (distinct? (vals log-files))
             {:type       ::log-files-local-name-collision
              :log-files  log-files})
    log-files))

(def noop
  "Does nothing."
  (reify DB
    (setup!    [db test node])
    (teardown! [db test node])))

(defn tcpdump
  "A database which runs a tcpdump capture from setup! to teardown!, and yields
  a `tcpdump` logfile. Options:

    :clients-only?  If true, applies a filter string which yields only traffic
                    from Jepsen clients, rather than capturing inter-DB-node
                    traffic.

    :filter A filter string to apply (in addition to ports).
            e.g. \"host 192.168.122.1\", which can be helpful for seeing *just*             client traffic from the control node.

    :ports  A collection of ports to grab traffic from."
  [opts]
  (let [dir      "/tmp/jepsen/tcpdump"
        log-file (str dir "/log")
        cap-file (str dir "/tcpdump")
        pid-file (str dir "/pid")]
    (reify
      DB
      (setup! [this test node]
        (control/su
          (control/exec :mkdir :-p dir)
          ; Combine custom, port, and client filters
          (let [filters (remove nil? [(->> (:ports opts)
                                           (map (partial str "port "))
                                           (str/join " or "))
                                      (when (:clients-only? opts)
                                        (str "host " (cn/control-ip)))
                                      (:filter opts)])
                filter-str (str/join " and " filters)]
            (cu/start-daemon!
              {:logfile log-file
               :pidfile pid-file
               :chdir   dir}
              "/usr/bin/tcpdump"
              :-w  cap-file
              :-s  65535
              :-B  16384 ; buffer in KB
              ; Theoretically, killing tcpdump with SIGINT should cause it to
              ; neatly flush its packets to disk and exit, but... as far as
              ; I can tell, it leaves the capture half-finished (and missing
              ; important packets from the end of the test!) no matter what?
              ; Let's try *not* buffering.
              :-U
              filter-str))))

      (teardown! [this test node]
        (control/su
          (when-let [pid (try+ (control/exec :cat pid-file)
                               (catch [:type :jepsen.control/nonzero-exit] e
                                 nil))]
            ; We want to get a nice clean exit here, if possible
            (meh (control/exec :kill :-s :INT pid))
            ; Wait for it to flush
            (while (try+ (control/exec :ps :-p pid)
                         true
                         (catch [:type :jepsen.control/nonzero-exit] e
                           false))
              (info "Waiting for tcpdump" pid "to exit")
              (Thread/sleep 50)))

          ; Okay, nuke it and clean up pidfile, etc
          (cu/stop-daemon! :tcpdump pid-file)
          (control/exec :rm :-rf dir)))

      LogFiles
      (log-files [db test node]
        {log-file "tcpdump.log"
         cap-file "tcpdump.pcap"}))))

(def cycle-tries
  "How many tries do we get to set up a database?"
  3)

(defn cycle!
  "Takes a test, and tears down, then sets up, the database on all nodes
  concurrently.

  If any call to setup! or setup-primary! throws :type ::setup-failed, we tear
  down and retry the whole process up to `cycle-tries` times."
  [test]
  (let [db (:db test)]
    (loop [tries cycle-tries]
      ; Tear down every node
      (info "Tearing down DB")
      (control/on-nodes test (partial teardown! db))

      ; Start up every node
      (if (= :retry (try+
                      ; Normal set up
                      (info "Setting up DB")
                      (control/on-nodes test (partial setup! db))

                      ; Set up primary
                      (when (satisfies? Primary db)
                        ; TODO: refactor primary out of core and here and
                        ; into util.
                        (info "Setting up primary" (first (:nodes test)))
                        (control/on-nodes test [(first (:nodes test))]
                                          (partial setup-primary! db)))

                      nil
                      (catch [:type ::setup-failed] e
                        (if (< 1 tries)
                          (do (info :throwable (pr-str (type (:throwable &throw-context))))
                              (warn (:throwable &throw-context)
                                    "Unable to set up database; retrying...")
                              :retry)

                          ; Out of tries, abort!
                          (throw+ e)))))
        (recur (dec tries))))))
