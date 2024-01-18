(ns jepsen.net.proto
  "Protocols for network manipulation. High-level functions live in
  jepsen.net.")

(defprotocol Net
  (drop! [net test src dest]
         "Drop traffic between nodes src and dest.")
  (heal! [net test]
         "End all traffic drops and restores network to fast operation.")
  (slow! [net test]
         [net test opts]
         "Delays network packets with options:

         ```clj
           {:mean          ; (in ms)
           :variance       ; (in ms)
           :distribution}  ; (e.g. :normal)
         ```")
  (flaky! [net test]
          "Introduces randomized packet loss")
  (fast!  [net test]
         "Removes packet loss and delays.")
  (shape! [net test nodes behavior]
          "Shapes network behavior,
          i.e. packet delay, loss, corruption, duplication, reordering, and rate
          for the given nodes."))

(defprotocol PartitionAll
  "This optional protocol provides support for making multiple network changes
  in a single call. If you don't support this protocol, we'll use drop!
  instead."
  (drop-all! [net test grudge]
             "Takes a grudge: a map of nodes to collections of nodes they
             should drop messages from, and makes the appropriate changes to
             the network."))
