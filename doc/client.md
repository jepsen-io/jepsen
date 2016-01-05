# Writing a client

A Jepsen *client* takes *invocation operations* and applies them to the system
being tested, returning corresponding *completion operations*. For our
Zookeeper test, we might model the system as a single register, stored in a
znode, storing an integer. The operations against that register might be
`read`, `write`, and `compare-and-set`, which we could model like so:

```clj
(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})
```

These functions can be used by `jepsen.generator` to construct new invocations
for reads, writes, and compare-and-set ops, respectively. Note that the read
value is `nil`--we don't know what value is being read until we actually
perform the read. When the client reads a particular value it'll fill it in for
the completion op.

Now we need to take these operations and *apply* them to zookeeper. We'll use
the [avout](http://avout.io) library, which provides a `zk-atom` backed by
zookeeper. We'll require `avout.core` and `jepsen.client`, and write a trivial
implementation of Jepsen's Client protocol...

```clj
(ns jepsen.zookeeper
  (:require [avout.core :as avout]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen [db    :as db]
                    [client  :as client]
                    [control :as c]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]))

(defn client
  "A client for a single compare-and-set register"
  []
  (reify client/Client
    (setup! [_ test node]
      (client))

    (invoke! [this test op])

    (teardown! [_ test])))

(defn zk-test
  [version]
  (assoc tests/noop-test
         :name    "zookeeper"
         :os      debian/os
         :db      (db version)
         :client  (client)))
```

Clients have a three-part lifecycle. We begin with a single dormant client
`(client)`, whose `setup!` function initializes a *fresh* client, and performs
any necessary setup work. We create fresh clients because each process needs an
independent client, talking to independent nodes. Once initialized, `invoke!`
lets our client handle operations. Finally, `teardown!` releases any resources
the client may be holding on to.

Our client doesn't hold any state yet, so we simply call `(client)` in `setup!`
to construct a fresh client for each process.

What state *do* we need for each client? In Avout, one makes a connection like
so:

```clj
(def conn (avout/connect "some-host"))
```

And using that connetion, Avout models a linearizable register like this:

```clj
; Construct a register at the path "/jepsen" with initial value 0.
(def a (avout/zk-atom conn "/jepsen" 0))
@a                    ; Read atom
(avout/reset!! a 1)   ; Reset the atom's value to 1
(avout/swap!! a inc)  ; Use the inc function to increment the atom's value
```

So we'll need two pieces of state in our client: a connection, and an atom.

```clj
(defn client
  "A client for a single compare-and-set register"
  [conn a]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (avout/connect (name node))
            a    (avout/zk-atom conn "/jepsen" 0)]
        (client conn a)))

    (invoke! [this test op])

    (teardown! [_ test]
      (.close conn))))

(defn zk-test
  [version]
  (assoc tests/noop-test
         :name    "zookeeper"
         :os      debian/os
         :db      (db version)
         :client  (client nil nil)))
```

Remember, the initial client *has no connections*--like a stem cell, it has the
*potential* to become an active client but doesn't do any work directly. We
call `(client nil nil)` to construct that initial client--its conn and atom
will be filled in when Jepsen calls `setup!`.

Running `lein test`, we can see the ZK connections opening and closing:

```bash
$ lein test
INFO  org.apache.zookeeper.ZooKeeper - Client environment:zookeeper.version=3.4.0-1202560, built on 11/16/2011 07:18 GMT
...
INFO  org.apache.zookeeper.ZooKeeper - Initiating client connection, connectString=n5 sessionTimeout=5000 watcher=zookeeper.internal$make_watcher$reify__7183@719567fd
INFO  org.apache.zookeeper.ZooKeeper - Initiating client connection, connectString=n1 sessionTimeout=5000 watcher=zookeeper.internal$make_watcher$reify__7183@63a650df
INFO  org.apache.zookeeper.ZooKeeper - Initiating client connection, connectString=n3 sessionTimeout=5000 watcher=zookeeper.internal$make_watcher$reify__7183@78007b36
INFO  org.apache.zookeeper.ZooKeeper - Initiating client connection, connectString=n2 sessionTimeout=5000 watcher=zookeeper.internal$make_watcher$reify__7183@1e5a45c3
INFO  org.apache.zookeeper.ZooKeeper - Initiating client connection, connectString=n4 sessionTimeout=5000 watcher=zookeeper.internal$make_watcher$reify__7183@21701c04
INFO  org.apache.zookeeper.ClientCnxn - Opening socket connection to server /192.168.122.13:2181
INFO  org.apache.zookeeper.ClientCnxn - Opening socket connection to server /192.168.122.11:2181
INFO  org.apache.zookeeper.ClientCnxn - Opening socket connection to server /192.168.122.14:2181
INFO  org.apache.zookeeper.ClientCnxn - Opening socket connection to server /192.168.122.12:2181
INFO  org.apache.zookeeper.ClientCnxn - Opening socket connection to server /192.168.122.15:2181
```

Now we have to actually *do* something with the client. Let's start with fifteen
seconds of reads, randomly staggered about a second apart. We'll pull in `jepsen.generator` to schedule operations, and `jepsen.util`'s timeout capability:

```clj
(ns jepsen.zookeeper
  (:require [avout.core :as avout]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen [db    :as db]
                    [client  :as client]
                    [control :as c]
                    [generator :as gen]
                    [tests :as tests]
                    [util :refer [timeout]]]
            [jepsen.os.debian :as debian]))
```

... and write a simple generator: take reads, stagger them by about a second,
give those operations to clients only (not the nemesis, which has other
duties), and stop after 15 seconds.

```clj
(defn zk-test
  [version]
  (assoc tests/noop-test
         :name    "zookeeper"
         :os      debian/os
         :db      (db version)
         :client  (client nil nil)
         :generator (->> r
                         (gen/stagger 1)
                         (gen/clients)
                         (gen/time-limit 15))))
```

This fails, because we haven't told the client *how* to intepret these
operations yet.

```bash
$ lein test
...
WARN  jepsen.core - Process 24 indeterminate
java.lang.AssertionError: Assert failed: (= (:process op) (:process completion))
```

The client's `invoke!` function needs to return a corresponding completion op,
with type `:ok` if the operation succeeded, `:fail` if it didn't take place, or
`:info` if we're not sure. `invoke!` can also throw an exception, which is
automatically converted to an `:info`.

```clj
    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read (assoc op :type :ok, :value @a))))
```

`timeout` allows 5000 milliseconds for its body to run, and if that time runs
out, it calls `(assoc op :type :info, :error :timeout)` to construct a default
value. We dispatch based on the `:f` field of the operation, and when it's a
`:read`, we take the invoke op and return a copy of it, with `:type` `:ok` and
a value obtained by reading the register `a`.

```bash
$ lein test
...
INFO  jepsen.util - 2 :invoke :read nil
INFO  jepsen.util - 2 :ok :read 0
INFO  jepsen.util - 2 :invoke :read nil
INFO  jepsen.util - 2 :ok :read 0
INFO  jepsen.util - 4 :invoke :read nil
INFO  jepsen.util - 4 :ok :read 0
```

Much better! We're successfully reading the initial value of 0. Let's add some
writes to the generator by replacing `r` with `(gen/mix [r w])`:

```clj
(defn zk-test
  [version]
  (assoc tests/noop-test
         :name    "zookeeper"
         :os      debian/os
         :db      (db version)
         :client  (client nil nil)
         :generator (->> (gen/mix [r w])
                         (gen/stagger 1)
                         (gen/clients)
                         (gen/time-limit 15))))
```

To handle those writes, we'll use `avout/reset!!`, and return the op with
`:type` `:ok`. If `reset!!` fails it'll throw, and Jepsen's machinery will
automatically convert it to an `:info` crash.

```clj
    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read  (assoc op :type :ok, :value @a)
                 :write (do (avout/reset!! a (:value op))
                            (assoc op :type :ok)))))
```

We'll confirm writes work by watching the test:

```bash
$ lein test
...
INFO  jepsen.util - 1 :ok :read 1
INFO  jepsen.util - 1 :invoke :read nil
INFO  jepsen.util - 1 :ok :read 1
INFO  jepsen.util - 2 :invoke :write  0
INFO  jepsen.util - 2 :ok :write  0
INFO  jepsen.util - 4 :invoke :write  2
INFO  jepsen.util - 4 :ok :write  2
INFO  jepsen.util - 3 :invoke :read nil
INFO  jepsen.util - 3 :ok :read 2
```

Seems reasonable! The final analysis is going to crash because we haven't told
it how to check the system yet, but we'll get to that later. First, we'll finish the client by adding compare-and-set to the mix:

```clj
      (gen/mix [r w cas])
```

Handling CaS is a little trickier. Avout gives us a `swap!!` function, which
takes a function `f` and updates the atom's value atomically to `(f
current-value)`, returning the new value.

```clj
    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read  (assoc op :type :ok, :value @a)
                 :write (do (avout/reset!! a (:value op))
                            (assoc op :type :ok))
                 :cas   (let [[value value'] (:value op)]
                          (avout/swap!! a (fn [current]
                                            (if (= current value)
                                              value'
                                              current)))
                          ; So... did it change or not?
                          ))))
```

The `let` binding here uses *destructuring*: it breaks apart the `[old-value
new-value]` pair from the operation's `:value` field into `value` and `value'`.
Our swap function compares the atom's current value to `value`, and if it's
equal, sets it to `value'`. Otherwise, it stays the same.

The tricky problem is that we have no way to know whether the CaS actually took
effect. We know the new value of the register, because `swap!!` returns it--but
we don't know if that's the *unchanged* value and the predicate comparison
failed, or if it's the *new* value and the predicate comparison succeeded. We
need another piece of mutable state to determine what happened. We'll use a
local Clojure atom (*not* an Avout atom, which is backed by Zookeeper!).

```clj
    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read  (assoc op :type :ok, :value @a)
                 :write (do (avout/reset!! a (:value op))
                            (assoc op :type :ok))
                 :cas   (let [[value value'] (:value op)
                              type           (atom :fail)]
                          (avout/swap!! a (fn [current]
                                            (if (= current value)
                                              (do (reset! type :ok)
                                                  value')
                                              (do (reset! type :fail)
                                                  current))))
                          (assoc op :type @type)))))
```

We bind `type` to a fresh atom, whose value is initially `:fail`. In each
branch of the swap function, we use `reset!` to set the atom to either `:ok` or
`:fail`. When it comes time to return a completion operation, we just read off
`type`'s current value.

```bash
$ lein test
INFO  jepsen.util - 2 :invoke :write  2
INFO  jepsen.util - 2 :ok :write  2
INFO  jepsen.util - 0 :invoke :cas  [2 3]
INFO  jepsen.util - 0 :ok :cas  [2 3]
INFO  jepsen.util - 1 :invoke :cas  [1 2]
INFO  jepsen.util - 1 :fail :cas  [1 2]
```

We expect some CaS ops to fail because their predicate value doesn't match the
current value, but a few (~1/5, since there are 5 possible values) should
succeed.

With our client performing operations, it's time to analyze results using a
[checker](checker.md).
