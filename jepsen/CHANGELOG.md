# Change Log

## 0.3.12

A big release! This fixes several deadlocks and a safety error in Elle's inference of version orders for rw workloads which caused tests to incorrectly report that version orders were cyclic. It also adds new features: more sophisticated transaction generation and checking for Elle, more robust, useful client setup/teardown, and better support for multi-role tests with dependencies between components. There are a slew of quality-of-life improvements: compact printing of operations, automatically inferring network devices, the ability to serialize byte arrays, initialize Jepsen's random seeds, flattening deeply nested checkers, and so on.

### Bugfixes

- `jepsen.history` had a deadlock in calls to `invocation` and `completion` for `SparseHistory`.
- `checker.perf` could crash when trying to render performance plots for histories where operations never completed. It now skips these operations.
- Under specific conditions, Elle could incorrectly infer that the version order for a key was cyclic despite no cycle existing. This bug was caused by the incorrect use of a mutable data structure for performance. It seems to have been rare: it hinged on a specific combination of search order and the specific `:index` values of operations, and took years to appear in Jepsen's tests.
- Elle failed to detect internal anomalies in rw and append workloads where a key's value was known to exist, but a read returned `nil`. It now catches these anomalies.
- `jepsen.role` allowed each DB setup thread to wait on their own individually-assigned barriers, causing deadlock during setup. It now generates a shared barrier for all nodes in a role on DB setup.
- `db.watchdog`'s `teardown!` would fail to tear down the wrapped DB when no watchdog existed.

### API Changes

- Client setup/teardown has been reworked. We now call `client/teardown!` at the start of each test, before `client/setup!`; this ensures that workloads start with a clean state when (e.g.) testing a hosted service. We create dedicated clients for setup/teardown, rather than using the same ones for the main `invoke` calls; this is more robust when a client (e.g.) decides its node is inaccessible and refuses to run `teardown!`. Finally, client teardown is skipped when testing with `--leave-db-running`; this ensures you can inspect DB state.
- Jepsen is now built for JDK 21+
- `control.util/ls-full` is deprecated; use `(ls dir {:full-path? true})`.
- A new namespace, `jepsen.print`, takes over printing and logging output. Many functions from `jepsen.util` now live here.

### New Features

- Operations now print as maps in `results.edn`, which makes things *much* more readable.
- Elle can now take a new `:transaction-order` key for checking read-write registers. This allows databases which know (or think they know) the order of transactions to specify it; Elle can use this to infer version orders and detect anomalies that would otherwise go hidden.
- Elle now supports a Zipfian distribution for the frequency of keys in the pool.
- Elle now generates a Zipfian distributed number of writes per key. Previously, the number of writes per key was constant, which caused tests to miss bugs that required frequent creation of new keys. Now we generate a variety of short-lived and long-lived keys in a single test. The defaults have been re-tuned to hopefully yield a good balance of long-lived-but-reasonably-debuggable histories.
- `role/db` can express dependencies between roles, making it easy to say "Don't start this service until postgres is up".
- `checker/flatten`: flattens deeply composed checkers.
- `jepsen.net` automatically infers which network device to use, rather than hardcoding `eth0`.
- `rand/set-rng!` and `set-seed!`: allows setting Jepsen's RNG once. Helpful for building deterministic test runs.

### Minor Changes

- We now use `dom.top.core/timeout` instead of `jepsen.util/timeout`. This seems significantly more robust when dealing with (e.g.) clients that swallow or retry InterruptedExceptions.
- `store.format` can now serialize byte arrays, so you can include them in histories and checker results.
- `control.util/await-tcp-port` can now take a host, if you'd like to wait on (e.g.) another node's TCP port.
- `control.util/stop-daemon` can now take a custom signal.
- `store/with-history!` now handles the history writer, as opposed to `generator.interpreter/run!`.
- `control.util/kill-bin!` can wait for the process it's killing.
- `lazyfs/mounted?`: a function to check if the filesystem is mounted.
- SSHJ's logging complaints about missing common keys are now suppressed.
- Fixed a `generator` docstring which mis-explained `:pending` ops.
- Improved tests for LazyFS.

[Full Changelog](https://github.com/jepsen-io/jepsen/compare/v0.3.11...v0.3.12)
