# jepsen.stolon

Jepsen tests for the Stolon Postgres replication system. Also happens to test single-node Postgres installs.

## Usage

To demonstrate that Postgres `SERIALIZABLE` isn't serializable, try

```
lein run test-all -w append --concurrency 50 --isolation serializable --just-postgres --node n1 --nemesis none --time-limit 120 -r 200 --test-count 2 --max-writes-per-key 16
```

If you have a postgres process on localhost, with a postgres user (and
database) named `jepsen`, and password `pw`, try:

```
lein run test-all -w append --max-writes-per-key 4 --concurrency 50 -r 500 --isolation serializable --time-limit 60 --nemesis none --existing-postgres --node localhost --no-ssh --postgres-user jepsen --postgres-password pw
```

## License

Copyright © 2020 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
