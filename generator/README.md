# jepsen.generator

This library provides the compositional generator system at the heart of
[Jepsen](https://jepsen.io). Generators produce a series of operations Jepsen
would like to perform against a system, like "Set key `x` to 3". They also
react to operations as they happen. For example, if the write fails, the
generator could decide to retry it.

In addition to the generators themselves, this library provides:

- `jepsen.random`: Pluggable random values
- `jepsen.generator.test`: Helpers for testing generators
- `jepsen.generator.context`: A high-performance, pure data structure which
  keeps track of the state used by generators
- `jepsen.generator.translation-table`: Maps worker threads to integers

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
