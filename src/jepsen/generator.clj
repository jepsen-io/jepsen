(ns jepsen.generator
  "Generates operations for a test. Generators are composable, stateful objects
  which emit operations for processes until they are exhausted, at which point
  they return nil. Generators may sleep when generating operations, to delay
  the rate at which the test proceeds

  Generators do *not* have to emit a :process for their operations; test
  workers will take care of that.")

(defprotocol Generator
  (op [generator test process] "Yields an operation to apply."))

(def void
  "A generator which terminates immediately"
  (reify Generator
    (op [generator test process])))

(def cas
  "Random cas/read ops for a compare-and-set register over a small field of
  integers."
  (reify Generator
    (op [generator test process]
      (if (< 0.5 (rand))
        {:type  :invoke
         :f     :read}
        {:type  :invoke
         :f     :cas
         :value [(rand-int 5) (rand-int 5)]}))))

(def queue
  "Random enqueue/dequeue operations."
  (reify Generator
    (op [gen test process]
      (if (< 0.5 (rand))
        {:type  :invoke
         :f     :enqueue
         :value (rand-int 1000)}
        {:type  :invoke
         :f     :dequeue}))))

(defn finite-count
  "Takes a generator and returns a generator which only produces n operations."
  [n gen]
  (let [life (atom (inc n))]
    (reify Generator
      (op [generator test process]
        (when (pos? (swap! life dec))
          (op gen test process))))))

(defn nemesis
  "Combines a generator of normal operations and a generator for nemesis
  operations into one. When the process requesting an operation is :nemesis,
  routes to the nemesis generator; otherwise to the normal generator."
  [nemesis-gen client-gen]
  (reify Generator
    (op [generator test process]
      (if (= :nemesis process)
        (op nemesis-gen test process)
        (op client-gen test process)))))
