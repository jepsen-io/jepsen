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

These are functions that construct Jepsen *operations*: an abstract
representation of things you can do to a database. `:invoke` means that we're
going to *try* an operation--when it completes, we'll use a type like `:ok` or
`:fail` to tell what happened. The `:f` tells us what function we're applying
to the database--for instance, that we want to perform a read or a write. These
can be *any* values--Jepsen doesn't know what they mean.

Function calls are parameterized by their arguments and return values. Jepsen
operations are parameterized by `:value`, which can be anything we like--Jepsen
doesn't inspect them. We use a write's value to specify the value that we
wrote, and a read's value to specify the value that we (eventually) read. When
we invoke a read, we don't know *what* we're going to read yet, so we'll leave
it `nil`.

These functions can be used by `jepsen.generator` to construct new invocations
for reads, writes, and compare-and-set ops, respectively. Note that the read
value is `nil`--we don't know what value is being read until we actually
perform the read. When the client reads a particular value it'll fill that
value in for the completion op.

## Connecting to the database

Now we need to take these operations and *apply* them to etcd. We'll use
the [Verschlimmbessergung](https://github.com/aphyr/verschlimmbesserung)
library to talk to etcd. We'll start by requiring Verschlimmbesserung, and
writing an empty implementation of Jepsen's Client protocol:

```clj
(ns jepsen.etcdemo
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
(defrecord Client [conn]
  client/Client
  (open! [this test node]
    this)

  (setup! [this test])

  (invoke! [_ test op])

  (teardown! [this test])

  (close! [_ test]))
```

`defrecord` defines a new type of data structure, which we're calling `Client`.
Each Client has a single field, called `conn`, which will hold our connection
to a particular network server. Clients support Jepsen's Client protocol, and
just like a `reify`, we'll provide implementations for Client functions.

Clients have a five-part lifecycle. We begin with a single *seed* client
`(client)`. When we call `open!` on that client, we get a *copy* of the client
bound to a particular node. The `setup!` function initializes any data
structures the test needs--for instance, creating tables or setting up
fixtures. `invoke!` applies operations to the system and returns corresponding
completion operations. `teardown!` cleans up any tables `setup!` may have
created. `close!` closes network connections and completes the lifecycle for
the client.

When it comes time to add the client to the test, we use `(Client.)` to
construct a new Client, and pass `nil` as the value for `conn`. Remember, our
initial seed client doesn't have a connection; Jepsen will call `open!` to get
connected clients later.

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name   "etcd"
          :os     debian/os
          :db     (db "v3.1.5")
          :client (Client. nil)}))
```

Now, let's complete our `open!` function by connecting to etcd. The
[Verschlimmbesserung docs](https://github.com/aphyr/verschlimmbesserung#usage)
tell us that every function takes a Verschlimmbesserung client, created with
`(connect url)`. That client is what we'll store in `conn`. Let's have
Verschlimmbesserung time out requests after 5 seconds, too.

```clj
(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (client-url node)
                                 {:timeout 5000})))

  (setup! [this test])

  (invoke! [_ test op])

  (teardown! [this test])

  (close! [_ test]
    ; If our connection were stateful, we'd close it here. Verschlimmmbesserung
    ; doesn't actually hold connections, so there's nothing to close.
    ))

(defn etcd-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "etcd"
          :os debian/os
          :db (db "v3.1.5")
          :client (Client. nil)}))
```

Remember, the initial client *has no connections*--like a stem cell, it has the
*potential* to become an active client but doesn't do any work directly. We
call `(Client. nil)` to construct that initial client--its conn will be filled
in when Jepsen calls `open!`.

## Reads

Now we have to actually *do* something with the client. Let's start with fifteen
seconds of reads, randomly staggered about a second apart. We'll pull in `jepsen.generator` to schedule operations.

```clj
(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [verschlimmbesserung.core :as v]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))
```

... and write a simple generator: take reads, stagger them by about a second,
give those operations to clients only (not the nemesis, which has other
duties), and stop after 15 seconds.

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "etcd"
          :os              debian/os
          :db              (db "v3.1.5")
          :client          (Client. nil)
          :generator       (->> r
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))}))
```

This throws a bunch of errors, because we haven't told the client *how* to
intepret these reads yet.

```bash
$ lein run test
...
WARN [2020-09-21 20:16:33,150] jepsen worker 0 - jepsen.generator.interpreter Process 0 crashed
clojure.lang.ExceptionInfo: throw+: {:type :jepsen.client/invalid-completion, :op {:type :invoke, :f :read, :value nil, :time 26387538, :process 0}, :op' nil, :problems ["should be a map" ":type should be :ok, :info, or :fail" ":process should be the same" ":f should be the same"]}
```

The client's `invoke!` function takes an invocation operation, and right now,
does nothing with it, returning `nil`. Jepsen is telling us that it should be a
map, and specifically one with a `:type` field, a matching `:process`, and a
matching `:f`. In short, we have to construct a completion operation which
*concludes* the invocation operation. We'll construct this completion op with
type `:ok` if the operation succeeded, `:fail` if it didn't take place, or
`:info` if we're not sure. `invoke!` can also throw an exception, which is
automatically converted to an `:info`.

Let's start by handling reads. We'll use `v/get` to read the value of a single
key. We can pick any name we like--let's call it "foo" for now.

```clj
    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (v/get conn "foo"))))
```

We dispatch based on the `:f` field of the operation, and when it's a
`:read`, we take the invoke op and return a copy of it, with `:type` `:ok` and
a value obtained by reading the register "foo".

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
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:pure-generators true
          :name            "etcd"
          :os              debian/os
          :db              (db "v3.1.5")
          :client          (Client. nil)
          :generator       (->> (gen/mix [r w])
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))}))
```

To handle those writes, we'll use `v/reset!`, and return the op with
`:type` `:ok`. If `reset!` fails it'll throw, and Jepsen's machinery will
automatically convert it to an `:info` crash.

```clj
    (invoke! [this test op]
               (case (:f op)
                 :read (assoc op :type :ok, :value (v/get conn "foo"))
                 :write (do (v/reset! conn "foo" (:value op))
                            (assoc op :type :ok))))
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
like to work with numbers. We could pull in a serialization library (jepsen
includes a simple one in `jepsen.codec`), but since we're only dealing with
integers and nil, we can get away with using Clojure's built-in `parse-long`
function. It doesn't like being passed `nil`, though, so we'll write a small
wrapper:

```clj
(defn parse-long-nil
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (parse-long s)))

...

  (invoke! [_ test op]
    (case (:f op)
      :read  (assoc op :type :ok, :value (parse-long-nil (v/get conn "foo")))
      :write (do (v/reset! conn "foo" (:value op))
                 (assoc op :type :ok))))
```

Note that we only call `parse-long` when our string is truthy--using `(when s
…)`. If `when` doesn't match, it'll return `nil`, which lets us pass through
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
  (invoke! [_ test op]
    (case (:f op)
      :read  (assoc op :type :ok, :value (parse-long-nil (v/get conn "foo")))
      :write (do (v/reset! conn "foo" (:value op))
                 (assoc op :type :ok))
      :cas (let [[old new] (:value op)]
             (assoc op :type (if (v/cas! conn "foo" old new)
                               :ok
                               :fail)))))
```

The `let` binding here uses *destructuring*: it breaks apart the `[old-value
new-value]` pair from the operation's `:value` field into `old` and `new`.
Since all values except `false` and `nil` are logically true, we can use the
result of the `cas!` call as our predicate in `if`.

## Handling exceptions

If you run this a few times, you might see:

```bash
$ lein run test
...
INFO [2017-03-30 22:38:51,892] jepsen worker 1 - jepsen.util 1  :invoke :cas  [3 1]
WARN [2017-03-30 22:38:51,936] jepsen worker 1 - jepsen.core Process 1 indeterminate
clojure.lang.ExceptionInfo: throw+: {:errorCode 100, :message "Key not found", :cause "/foo", :index 11, :status 404}
  at slingshot.support$stack_trace.invoke(support.clj:201) ~[na:na]
  ...
```

If we try to CaS the key before it's written, Verschlimmbesserung will throw an
exception complaining (quite sensibly!) that we can't alter something that
doesn't exist. This won't cause our test to return false positives--Jepsen will
interpret the exception as an indeterminate `:info` result, and allow that it
might or might not have taken place. However, we *know* the value didn't change
when we get this exception, so we can convert it to a known failure. We'll pull
in the `slingshot` exception handling library, so we can catch that particular
error code.

```clj
(ns jepsen.etcdemo
  (:require ...
            [slingshot.slingshot :refer [try+]]))
```

... and wrap our `cas` in a try/catch block.

```clj
    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (parse-long-nil (v/get conn "foo")))
        :write (do (v/reset! conn "foo" (:value op))
                   (assoc op :type :ok))
        :cas (try+
               (let [[old new] (:value op)]
                 (assoc op :type (if (v/cas! conn "foo" old new)
                                   :ok
                                   :fail)))
               (catch [:errorCode 100] ex
                 (assoc op :type :fail, :error :not-found)))))
```

This [:errorCode 100] form tells Slingshot to catch only exceptions which have
that particular error code, and bind them to `ex`. We've added an extra
`:error` field to our operation. This doesn't matter as far as correctness is
concerned, but it helps us understand what happened when we read the logs.
Jepsen prints errors at the end of log lines.

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
[checker](04-checker.md).
