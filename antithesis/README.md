# jepsen.antithesis

This library supports running Jepsen tests inside Antithesis environments. It
provides entropy, lifecycle hooks, and assertions.

## Randomness

You should wrap your entire program in `(with-rng ...)`. This does nothing in
ordinary environments, but in Antithesis, it replaces the jepsen.random RNG
with one powered by Antithesis.

## Lifecycle

Call `setup-complete!` once the test is ready to begin. Call `event!` to
signal interesting things have happened.

## Assertions

Assertions begin with `assert-`, and take an expression, a message, and data
to include if the assertion fails. For instance:

(assert-always! (not (db-corrupted?)) \"DB corrupted\" {:db \"foo\"})"

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
