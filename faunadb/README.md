# Jepsen FaunaDB Tests

A Jepsen test suite for FaunaDB. Designed for version 2.5.4 through 2.6.0.rc9.

Tests:

- Bank: Tests snapshot isolation over instance and index reads, with fixed and
  dynamic pools of instances, with both standard and temporal queries.
- G2: Looks for predicate anti-dependency cycles
- Internal: Validates that modifications within a transaction are visible to
  later queries within that transaction
- Monotonic: Checks for cases where single processes observe the value of an
  increment-only register going backwards, and for cases where the relationship
  between timestamps and register values is nonmonotonic.
- Multimonotonic: Checks for non-monotonic timestamp<->value relationships with
  multi-instance reads.
- Pages: Inserts n instances in a single transaction and validates that you can
  read all n atomically with pagination.
- Register: linearizable single registers.
- Set: Inserts instances into a set and validates that they're all readable by
  index queries

## Usage

See `lein run test --help` for all options.

To run the full test suite on version 2.6.0.rc9, with 1000 seconds per test,
and 8 threads per node.

```
lein run test-all --wait-for-convergence --version 2.6.0.rc9 --time-limit 1000 --concurrency 8n
```

To demonstrate read skew in the bank test with temporal queries on 2.5.5:

```
lein run test --wait-for-convergence --version 2.5.5 --time-limit 600 --concurrency 8n --at-query
```

Or catastrophic read skew in 2.6.0.rc9:

```
lein run test-all --wait-for-convergence --version 2.6.0.rc9 -w bank --concurrency 8n --nemesis stop --nemesis-interval 10 --time-limit 600 --test-count 5
```

## License

Copyright © 2018 Fauna, Inc & Jepsen, LLC.

Distributed under the Eclipse Public License either version 1.0 or (at your
option) any later version.
