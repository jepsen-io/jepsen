# Aerospike

Jepsen tests for the aerospike database.

## Usage

Demonstrate data loss with concurrent hard crashes:

```bash
lein run test --username admin --nodes-file ~/nodes --workload set -time-limit 120 --concurrency 100 --no-partitions --no-clocks --max-dead-nodes 5
```

Demonstrate data loss with sequential hard crashes followed by recovery

## License

Copyright © 2015, 2017, 2018 Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
