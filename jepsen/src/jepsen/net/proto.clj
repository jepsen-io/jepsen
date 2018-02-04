(ns jepsen.net.proto
  "Protocols for network manipulation. High-level functions live in
  jepsen.net.")

(defprotocol PartitionAll
  "This optional protocol provides support for making multiple network changes
  in a single call. If you don't support this protocol, we'll use drop!
  instead."
  (drop-all! [net test grudge]
             "Takes a grudge: a map of nodes to collections of nodes they
             should drop messages from, and makes the appropriate changes to
             the network."))
