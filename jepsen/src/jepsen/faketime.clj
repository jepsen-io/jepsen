(ns jepsen.faketime
  "Libfaketime is useful for making clocks run at differing rates! This
  namespace provides utilities for stubbing out programs with faketime."
  (:require [jepsen.control :as c]))

(defn faketime-script
  "A sh script which invokes cmd with a faketime wrapper."
  [cmd]
  (str "#!/bin/bash\n"
       "faketime -m -f \"+$((RANDOM%100))s x1.${RANDOM}\" "
       (c/expand-path cmd)
       " \"$@\""))

(defn faketime-wrapper!
  "Replaces an executable with a faketime wrapper, moving the original to
  x.no-faketime. Idempotent."
  [cmd]
  (let [cmd'    (str cmd ".no-faketime")
        wrapper (faketime-script cmd')]
    (when-not (cu/exists? cmd')
      (info "Installing faketime wrapper.")
      (c/exec :mv cmd cmd')
      (c/exec :echo wrapper :> cmd)
      (c/exec :chmod "a+x" cmd))))
