(ns jecci.utils.db
  "Some handy little functions for db"
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [slingshot.slingshot :refer [throw+ try+]]
            [jecci.utils.handy :as juh]
            [jepsen
             [util :as util]
             [control :as c]]))

(defn restart-loop*
  "TiDB, and actually many other MPP DBMS are fragile on startup... processes love to crash if they can't complete
  their initial requests to network dependencies. TiDB try to work around this by
  checking whether the daemon is running, and restarting it if necessary. Let`s just 
	reuse it here.

  Takes a name (used for error messages and logging), a function which starts
  the node, and a status function which returns one of three states:

  :starting - The node is still starting up
  :ready    - The node is (hopefully) ready to serve requests
  :crashed  - The node crashed

  We call start, then poll the status function until we see :ready or :crashed.
  If ready, returns. If :crashed, restarts the node and tries again."
  [name start! get-status]
  (let [deadline (+ (util/linear-time-nanos) (util/secs->nanos 300))]
    ; First startup!
    (start!)

    (loop [status :init]
      (when (< deadline (util/linear-time-nanos))
        ; Out of time
        (throw+ {:type :restart-loop-timed-out, :service name}))

      (when (= status :crashed)
        ; Need to restart
        (info name "crashed during startup; restarting")
        (start!))

      ; Give it a bit
      (Thread/sleep 1000)

      ; OK, how's it doing?
      (let [status (get-status)]
        (if (= status :ready)
          ; Done
          status
          ; Still working
          (recur status))))))

(defmacro restart-loop
  "Macro form of restart-loop*: takes two forms instead of two functions."
  [name start! get-status]
  `(restart-loop* ~name
                  (fn ~'start!     [] ~start!)
                  (fn ~'get-status [] ~get-status)))

(defn my-install-archive!
  "If working inside a company or organization, it is very possible that
   we need to use some proxy to get the tar, which requires using wget with
   some sort of environment variable. Yet it is not supported by jepsen yet,
   so use a simple function here as replacement. Will probably change in the 
   future"
  [url [& wget-opts] download dst-dir]
  (c/sudo "jecci"
        (c/exec "sudo" :rm :-rf download)
        (c/exec "mkdir" :-p download)
        (c/exec "sudo" :rm :-rf dst-dir)
        (c/exec "mkdir" :-p dst-dir)
        (c/cd download
              (info "wgetting url")
              (juh/exec->info (c/exec :wget url wget-opts))
              (info "tar received")
              (c/exec "bash" :-c (str "ls|xargs -I gz "
                                      (cond (str/ends-with? url ".tar.gz") "tar -zxvf gz -C "
                                            (str/ends-with? url ".zip") "unzip gz -C ")
                                      dst-dir " --strip-components=1")))
        (juh/exec->info (c/exec "ls" "-l" dst-dir))))

(defn my-copy-archive!
  "Copies everything inside src-dir into dst-dir. Again, if you cannot access
   the tarball-url, just use this work around"
  [dst-dir src-dir]
  (c/exec "mkdir" :-p dst-dir)
  (c/exec :bash :-c (str "cp" " -r " (str src-dir "/* ") dst-dir))
  (juh/exec->info (c/exec "ls" "-l" dst-dir)))