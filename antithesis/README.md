# jepsen.antithesis

This library supports running Jepsen tests inside Antithesis environments. It
provides entropy, lifecycle hooks, and assertions.

## Installation

[![Clojars Project](https://img.shields.io/clojars/v/io.jepsen/antithesis.svg)](https://clojars.org/io.jepsen/antithesis)

From Clojars, as usual. Note that the Antithesis SDK pulls in an ancient
version of Jackson and *needs it*, so in your `project.clj`, you'll likely want
to prevent other dependencies from relying on Jackson:

```clj
  :dependencies [...
                 [io.jepsen/antithesis "0.1.0"]
                 [jepsen "0.3.10"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-core]]]

```

## Usage

The main namespace is [`jepsen.antithesis`](src/jepsen/antithesis.clj). There
are several things you can do to integrate your test into Antithesis.

### Randomness

First, wrap the entire program in `(antithesis/with-rng ...)`. This does
nothing in ordinary environments, but in Antithesis, it replaces the
jepsen.random RNG with the Antithesis SDK's entropy source.

### Wrapping Tests

Second, wrap the entire test map with `(antithesis/test test)`. In an
Antithesis run, this disables the OS, DB, and SSH connections.

## Clients

Wrap your client in `(antithesis/client your-client)`. This client informs
Antithesis that the setup is complete, and makes assertions about each
invocation and completion.

## Checker

You can either make assertions (see below) by hand inside your checkers, or you
can wrap an existing checker in `(antithesis/checker "some name" checker)`.
This asserts that the checker's results are always `:valid? true`. You can also
use `antithesis/checker+` to traverse a tree of checkers, wrapping each one
with assertions.

## Generator

Instead of a time limit, you can limit your generator with something like:

```clj
(if (antithesis/antithesis?)
  (antithesis/early-termination-generator
   {:interval 100
    :probability 0.1}
   my-gen)
  (gen/time-limit ... my-gen))
```

This early-termination-generator flips a coin every 10 operations, deciding
whether to continue. This allows Antithesis to perform some long runs and some
short ones. I'm not totally sure whether this is a good idea yet, but it does
seem to get us to much shorter reproducible histories.

## Lifecycle

If you'd like to manage the lifecycle manually, you can Call `setup-complete!`
once the test is ready to begin--for instance, at the end of `Client/setup!`.
Call `event!` to signal interesting things have happened.

## Assertions

Assertions begin with `assert-`, and take an expression, a message, and data
to include if the assertion fails. For instance:

```clj
(assert-always! (not (db-corrupted?))
                "DB corrupted" {:db "foo"})
```

Ideally, you want to do these *during* the test run, so Antithesis can fail
fast. Many checks can only be done with the full history, by the checker; for
these, assert test validity in the checker itself:

```clj
(defrecord MyChecker []
  (check [_ test history opts]
    ...
    (a/assert-always (true? valid?) "checker valid" a)
    {:valid? valid? ...}))
```

## License

Copyright © Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
https://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
