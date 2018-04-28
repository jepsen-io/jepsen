(ns jepsen.os
  "Controls operating system setup and teardown.")

(defprotocol OS
  (setup!     [os test node] "Set up the operating system on this particular
                             node.")
  (teardown!  [os test node] "Tear down the operating system on this particular
                             node.")
  (install-build-essential! [os] "Install build tools."))

(def noop
  "Does nothing"
  (reify OS
         (setup!    [os test node])
         (teardown! [os test node])
         (install-build-essential! [os])))
