# Writing a client

A Jepsen *client* takes *invocation operations* and applies them to the system
being tested, returning corresponding *completion operations*. For our
etcd test, we might model the system as a single register: a particular key
holding an integer. The operations against that register might be `read`,
`write`, and `compare-and-set`, which we could model like so:

```clj
(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})
```

These functions can be used by `jepsen.generator` to construct new invocations
for reads, writes, and compare-and-set ops, respectively. Note that the read
value is `nil`--we don't know what value is being read until we actually
perform the read. When the client reads a particular value it'll fill that
value in for the completion op.

## Connecting to the database

Now we need to take these operations and *apply* them to etcd. We'll use
the [Verschlimmbessergung](https://github.com/aphyr/verschlimmbesserung)
library to talk to etcd. We'll start by requireing Verschlimmbesserung, and
writing an empty implementation of Jepsen's Client protocol:

```clj
(ns jepsen.etcdemo
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
...

(defn client
  "A client for a single compare-and-set register"
  []
  (reify client/Client
    (setup! [_ test node]
      (client))

    (invoke! [this test op])

    (teardown! [_ test])))

(defn etcd-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :client (client)}
         opts))
```

Clients have a three-part lifecycle. We begin with a single dormant client
`(client)`, whose `setup!` function initializes a *fresh* client, and performs
any necessary setup work. We create fresh clients because each process needs an
independent client, talking to independent nodes. Once initialized, `invoke!`
lets our client handle operations. Finally, `teardown!` releases any resources
the client may be holding on to.

Our client doesn't hold any state yet, so we simply call `(client)` in `setup!`
to construct a fresh client for each process.

What state *do* we need for each client? The [Verschlimmbesserung docs](https://github.com/aphyr/verschlimmbesserung#usage) tell us that every function takes a Verschlimmbesserung client, created with `(connect url)`.

So we'll need one piece of state in our client: the connection to etcd. When we
set up a fresh client, we'll open a connection to that node, and return a new
client wrapping that connection. Let's have Verschlimmbesserung time out
requests after 5 seconds, too.

```clj
(defn client
  "A client for a single compare-and-set register"
  [conn]
  (reify client/Client
    (setup! [_ test node]
      (client (v/connect (client-url node)
                         {:timeout 5000})))

    (invoke! [this test op])

    (teardown! [_ test]
      ; If our connection were stateful, we'd close it here.
      ; Verschlimmbesserung doesn't hold a connection open, so we don't need to
      ; close it.
      )))


(defn etcd-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :client (client nil)}
         opts))
```

Remember, the initial client *has no connections*--like a stem cell, it has the
*potential* to become an active client but doesn't do any work directly. We
call `(client nil)` to construct that initial client--its conn and atom
will be filled in when Jepsen calls `setup!`.

## Reads

Now we have to actually *do* something with the client. Let's start with fifteen
seconds of reads, randomly staggered about a second apart. We'll pull in `jepsen.generator` to schedule operations, and `jepsen.util` for a few odds and ends.

```clj
(ns jepsen.etcdemo
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
```

... and write a simple generator: take reads, stagger them by about a second,
give those operations to clients only (not the nemesis, which has other
duties), and stop after 15 seconds.

```clj
(defn etcd-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :client (client nil)
          :generator (->> r
                          (gen/stagger 1)
                          (gen/clients)
                          (gen/time-limit 15))}
         opts))
```

This throws a bunch of errors, because we haven't told the client *how* to
intepret these reads yet.

```bash
$ lein run test
...
WARN  jepsen.core - Process 24 indeterminate
java.lang.AssertionError: Assert failed: (= (:process op) (:process completion))
```

The client's `invoke!` function takes an invocation operation, and right now,
does nothing with it. We need to return a corresponding completion op, with
type `:ok` if the operation succeeded, `:fail` if it didn't take place, or
`:info` if we're not sure. `invoke!` can also throw an exception, which is
automatically converted to an `:info`.

Let's start by handling reads. We'll use `v/get` to read the value of a single
key. We can pick any name we like--let's call it "r" for "register".

```clj
    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (v/get conn "r")))))
```

We dispatch based on the `:f` field of the operation, and when it's a
`:read`, we take the invoke op and return a copy of it, with `:type` `:ok` and
a value obtained by reading the register "r".

```bash
$ lein run test
...
INFO [2017-03-30 15:28:17,423] jepsen worker 2 - jepsen.util 2  :invoke :read nil
INFO [2017-03-30 15:28:17,427] jepsen worker 2 - jepsen.util 2  :ok :read nil
INFO [2017-03-30 15:28:18,315] jepsen worker 0 - jepsen.util 0  :invoke :read nil
INFO [2017-03-30 15:28:18,320] jepsen worker 0 - jepsen.util 0  :ok :read nil
INFO [2017-03-30 15:28:18,437] jepsen worker 4 - jepsen.util 4  :invoke :read nil
INFO [2017-03-30 15:28:18,441] jepsen worker 4 - jepsen.util 4  :ok :read nil
```

Much better! We haven't created the key yet, so its value is `nil`. In order to
change the value, we'll add some some writes to the generator.

## Writes

We'll change our generator to take a random mixture of reads and writes, using
`(gen/mix [r w])`.

```clj
(defn etcd-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :client (client nil)
          :generator (->> (gen/mix [r w])
                          (gen/stagger 1)
                          (gen/clients)
                          (gen/time-limit 15))}
         opts))
```

To handle those writes, we'll use `v/reset!`, and return the op with
`:type` `:ok`. If `reset!` fails it'll throw, and Jepsen's machinery will
automatically convert it to an `:info` crash.

```clj
    (invoke! [this test op]
               (case (:f op)
                 :read (assoc op :type :ok, :value (v/get conn "r"))
                 :write (do (v/reset! conn "r" (:value op))
                            (assoc op :type, :ok))))
```

We'll confirm writes work by watching the test:

```bash
$ lein run test
INFO [2017-03-30 22:14:25,428] jepsen worker 4 - jepsen.util 4  :invoke :write  0
INFO [2017-03-30 22:14:25,439] jepsen worker 4 - jepsen.util 4  :ok :write  0
INFO [2017-03-30 22:14:25,628] jepsen worker 0 - jepsen.util 0  :invoke :read nil
INFO [2017-03-30 22:14:25,633] jepsen worker 0 - jepsen.util 0  :ok :read "0"
```

Ah, we've got a bit of a snag here. etcd thinks in terms of strings, but we'd
like to work with numbers. We could pull in a JSON parser like
[Cheshire](https://github.com/dakrone/cheshire), but since we only care about
integers and null, we can get away with using Java's built-in
`Long.parseLong(String str)` method.

```clj
(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

...

    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (parse-long (v/get conn "r")))
        :write (do (v/reset! conn "r" (:value op))
                   (assoc op :type, :ok))))
```

Note that we only call parseLong when our string is truthy--using `(when s
...)`. If `when` doesn't match, it'll return `nil`, which lets us pass through
`nil` values transparently.

```bash
$ lein run test
...
INFO [2017-03-30 22:26:45,322] jepsen worker 4 - jepsen.util 4  :invoke :write  1
INFO [2017-03-30 22:26:45,341] jepsen worker 4 - jepsen.util 4  :ok :write  1
INFO [2017-03-30 22:26:45,434] jepsen worker 2 - jepsen.util 2  :invoke :read nil
INFO [2017-03-30 22:26:45,439] jepsen worker 2 - jepsen.util 2  :ok :read 1
```

Seems reasonable! Only one type of operation left to implement: compare-and-set.

## Compare and set

We'll finish the client by adding compare-and-set to the mix:

```clj
      (gen/mix [r w cas])
```

Handling CaS is a little trickier. Verschlimmbesserung gives us a `cas!`
function, which takes a connection, key, old value, and new value. `cas!` sets
the key to the new value if and only if the old value matches what's currently
there, and returns a detailed response map. If the CaS fails, it returns false.
We can use that to determine the `:type` of the CaS operation.

```clj
    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (parse-long (v/get conn "r")))
        :write (do (v/reset! conn "r" (:value op))
                   (assoc op :type, :ok))
        :cas (let [[value value'] (:value op)]
               (assoc op :type (if (v/cas! conn "r" value value')
                                 :ok
                                 :fail)))))
```

The `let` binding here uses *destructuring*: it breaks apart the `[old-value
new-value]` pair from the operation's `:value` field into `value` and `value'`.
Since all values except `false` and `nil` are logically true, we can use the
result of the `cas!` call as our predicate in `if`.

## Handling exceptions

```bash
$ lein run test
...
INFO [2017-03-30 22:38:51,892] jepsen worker 1 - jepsen.util 1  :invoke :cas  [3 1]
WARN [2017-03-30 22:38:51,936] jepsen worker 1 - jepsen.core Process 1 indeterminate
clojure.lang.ExceptionInfo: throw+: {:errorCode 100, :message "Key not found", :cause "/r", :index 11, :status 404}
  at slingshot.support$stack_trace.invoke(support.clj:201) ~[na:na]
  ...
```

A slight hiccup: if we try to CaS the key before it's written,
Verschlimmbesserung will throw an exception complaining (quite sensibly!) that
we can't alter something that doesn't exist. This won't cause our test to
return false positives--Jepsen will interpret the exception as an indeterminate
`:info` result, and allow that it might or might not have taken place. However,
we *know* the value didn't change when we get this exception, so we can convert
it to a known failure. We'll pull in the `slingshot` exception handling library, so we can catch that particular error code.

```clj
(ns jepsen.etcdemo
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [slingshot.slingshot :refer [try+]]
            ...))
```

... and wrap our `cas` in a try/catch block.

```clj
    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (parse-long (v/get conn "r")))
        :write (do (v/reset! conn "r" (:value op))
                   (assoc op :type, :ok))
        :cas (try+
               (let [[value value'] (:value op)]
                 (assoc op :type (if (v/cas! conn "r" value value'
                                             {:prev-exist? true})
                                   :ok
                                   :fail)))
               (catch [:errorCode 100] _
                 (assoc op :type :fail, :error :not-found)))))
```

We've added an extra `:error` field to our operation. This doesn't matter to
Jepsen as far as correctness is concerned, but it helps us understand what
happened when we read the logs.

```bash
$ lein run test
...
INFO [2017-03-30 23:00:50,978] jepsen worker 0 - jepsen.util 0  :invoke :cas  [1 4]
INFO [2017-03-30 23:00:51,065] jepsen worker 0 - jepsen.util 0  :fail :cas  [1 4] :not-found
```

Much neater. In general, we'll start by writing the simplest code we can, and
allow Jepsen to handle exceptions for us. Once we have a feeling for how things
can go wrong, we can introduce special error handlers and semantics for those
failure cases.

```bash
...
INFO [2017-03-30 22:38:59,278] jepsen worker 1 - jepsen.util 11 :invoke :write  4
INFO [2017-03-30 22:38:59,286] jepsen worker 1 - jepsen.util 11 :ok :write  4
INFO [2017-03-30 22:38:59,289] jepsen worker 4 - jepsen.util 4  :invoke :cas  [2 2]
INFO [2017-03-30 22:38:59,294] jepsen worker 1 - jepsen.util 11 :invoke :read nil
INFO [2017-03-30 22:38:59,297] jepsen worker 1 - jepsen.util 11 :ok :read 4
INFO [2017-03-30 22:38:59,298] jepsen worker 4 - jepsen.util 4  :fail :cas  [2 2]
INFO [2017-03-30 22:38:59,818] jepsen worker 4 - jepsen.util 4  :invoke :write  1
INFO [2017-03-30 22:38:59,826] jepsen worker 4 - jepsen.util 4  :ok :write  1
INFO [2017-03-30 22:38:59,917] jepsen worker 1 - jepsen.util 11 :invoke :cas  [1 2]
INFO [2017-03-30 22:38:59,926] jepsen worker 1 - jepsen.util 11 :ok :cas  [1 2]
```

Notice that some CaS operations fail, and other succeed? It's OK for them to
fail, and in fact, it's desirable. We expect some CaS ops to fail because their
predicate value doesn't match the current value, but a few (~1/5, since there
are 5 possible values for the register at any given point) should succeed. In
addition, it's worth trying operations we think should be impossible, because
if they *do* succeed, that can point to a consistency violation.

With our client performing operations, it's time to analyze results using a
[checker](checker.md).
