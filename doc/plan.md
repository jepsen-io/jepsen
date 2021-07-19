# Stuff to improve!

## Error handling

- Knossos: Better error messages when users pass models that fail on the
  first op (I think there's a ticket about this? Null pointer exception for i?)
- When users enter a node multiple times into :nodes, complain early

## Visualizations

- Add a plot for counters, showing the upper and lower bounds, and the observed
  value
- Rework latency plot color scheme to use colors that hint at a continuum
- Adaptive temporal resolution for rate and latency plots, based on point
  density
- Where plots are dense, make points somewhat transparent to better show
  density?

## Web

- Use utf-8 for transferring files; I think we're doing latin-1 or ascii or
  8859-1 or something now.
- Add search for tests
- Add sorting
- Add filtering

## Performance

- Knossos: let's make the memoization threshold configurable via options passed
  to the checker.

## Core

- Macro like (synchronize-nodes test), which enforces a synchronization
  barrier where (count nodes threads) must come to sync on the test map.
- Generator/each works on each *process*, not each *thread*, but almost always,
  what people intend is for each thread--and that's how concat, independent,
  etc work. This leads to weird scenarios like tests looping on a final read
  forever and ever, as each process crashes, a new one comes in and gets a
  fresh generator. Let's make it by thread?

## Extensions

- Reusable packet capture utility (take from cockroach)

## New tests

- Port bank test from Galera into core (alongside G2)
- Port query-across-tables-and-insert from Cockroach into core
- Port pure-insert from Cockroach into core
- Port comments from Cockroach into core (better name?)
- Port other Hermitage tests to Jepsen?

## Tests
- Clean up causal test. Drop model and port to workload
