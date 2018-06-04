# Jepsen.txn

Support library for generating and analyzing transactional, multi-object
histories. This is very much a work in progress.

## Concepts

A *state* is a map of keys to values.

```clj
{:x 1
 :y 2}
```

Our data model is a set of *stateful objects*. An *object* is a uniquely named
register. Given a state, each object's value is given by that state's value for
the object's key.

A *micro-op* is a primitive atomic transition over a state. We call these
"micro" to distinguish them from "ops" in Jepsen. In this library, however,
we'll use *op* as a shorthand for micro-op unless otherwise specified.

```clj
[:r :x 1] ; Read the value of x, finding 1
[:w :y 2] ; Write 2 to y
```

A *transaction* is an ordered sequence of micro-ops.

```clj
[[:w :x 1] [:r :x 1] [:w :y 2]] ; Set x to 1, read that write, set y to 2
```

A *sequential history* is an ordered sequence of transactions.

```clj
[[[:w :x 1] [:w :y 2]] ; Set x and y to 1 and 2
 [[:r :x 1]]           ; Observe x = 1
 [[:r :y 2]]]          ; Observe y = 2
```

A *history* is a concurrent history of Jepsen operations, each with an
arbitrary :f (which could be used to hint at the purpose or class of the
transaction being performed), and whose value is a transaction.

```clj
; A concurrent write of x=1 and read of x=1
[{:process 0, :type :invoke, :f :txn, :value [[:w :x 1]]}
 {:process 1, :type :invoke, :f :txn, :value [[:r :x nil]]}
 {:process 0, :type :invoke, :f :txn, :value [[:w :x 1]]}
 {:process 1, :type :invoke, :f :txn, :value [[:r :x 1]]}]
```

An *op interpreter* is a function that takes a state and a micro-op, and
applies that operation to the state. It returns [state' op']: the resulting
state, and the op with any missing values (e.g. reads) filled in.

A *simulator* simulates the effect of executing transactions on some example
system. It takes an initial state, a sequence of operations, and produces a
history by applying those operations to the system. It may simulate
singlethreaded or multithreaded execution, so long as each process's effects
are singlethreaded. Simulators are useful for generating randomized histories
which are known to conform to some consistency model, such as serializability
or snapshot isolation, and those histories can be used to test programs that
verify those properties.

## License

Copyright © 2018 Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
