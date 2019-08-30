(ns jepsen.faketime
  "Libfaketime is useful for making clocks run at differing rates! This
  namespace provides utilities for stubbing out programs with faketime."
  (:require [clojure.tools.logging :refer :all]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]))

(defn install-0.9.6-jepsen1!
  "Installs our fork of 0.9.6 (the last version which worked with jemalloc),
  which includes a patch to support CLOCK_MONOTONIC_COARSE and
  CLOCK_REALTIME_COARSE. Gosh, this is SUCH a hack."
  []
  (c/su
    (c/exec :mkdir :-p "/tmp/jepsen")
    (c/cd "/tmp/jepsen"
          (when-not (cu/exists? "libfaketime-jepsen")
            (c/exec :git :clone "https://github.com/jepsen-io/libfaketime.git"
                    "libfaketime-jepsen"))
          (c/cd "libfaketime-jepsen"
                (c/exec :git :checkout "0.9.6-jepsen1")
                (c/exec :make)
                (c/exec :make :install)))))

(defn script
  "A sh script which invokes cmd with a faketime wrapper. Takes an initial
  offset in seconds, and a clock rate to run at."
  [cmd init-offset rate]
  (let [init-offset (long init-offset)
        rate        (float rate)]
    (str "#!/bin/bash\n"
         "faketime -m -f \""
         (if (neg? init-offset) "-" "+") init-offset "s x" rate "\" "
         (c/expand-path cmd)
         " \"$@\"")))

(defn wrap!
  "Replaces an executable with a faketime wrapper, moving the original to
  x.no-faketime. Idempotent."
  [cmd init-offset rate]
  (let [cmd'    (str cmd ".no-faketime")
        wrapper (script cmd' init-offset rate)]
    (if (cu/exists? cmd')
      (do (info "Installing faketime wrapper.")
          (c/exec :echo wrapper :> cmd))
      (do (c/exec :mv cmd cmd')
          (c/exec :echo wrapper :> cmd)))
    (c/exec :chmod "a+x" cmd)))

(defn unwrap!
  "If a wrapper is installed, remove it and replace it with the original
  .nofaketime version of the binary."
  [cmd]
  (let [cmd' (str cmd ".no-faketime")]
    (when (cu/exists? cmd')
      (c/exec :mv cmd' cmd))))

(defn rand-factor
  "Helpful for choosing faketime rates. Takes a factor (e.g. 2.5) and produces
  a random number selected from a distribution around 1, with minimum and
  maximum constrained such that factor * min = max. Intuitively, the fastest
  clock can be no more than twice as fast as the slowest."
  [factor]
  (let [max (/ 2 (+ 1 (/ factor)))
        min (/ max factor)]
    (+ min (rand (- max min)))))
