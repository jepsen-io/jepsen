# jepsen.antithesis

This library supports running Jepsen tests inside Antithesis environments. It
provides entropy, lifecycle hooks, and assertions.

## Installation

From Clojars, as usual. Note that the Antithesis SDK pulls in an ancient
version of Jackson, so in your `project.clj`, you'll likely want to prevent
`io.jepsen/antithesis` from pulling that in:

```clj
  :dependencies [...
                 [io.jepsen/antithesis "0.1.0"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind]]
```

The main namespace is [`jepsen.antithesis`](src/jepsen/antithesis.clj).

## Randomness

You should wrap your entire program in `(with-rng ...)`. This does nothing in
ordinary environments, but in Antithesis, it replaces the jepsen.random RNG
with one powered by Antithesis.

## Lifecycle

Call `setup-complete!` once the test is ready to begin--for instance, at the
end of `Client/setup!`. Call `event!` to signal interesting things have
happened.

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
