# Stuff to improve!

## Error handling

- Knossos: Better error messages when users pass models that fail on the
  first op (I think there's a ticket about this? Null pointer exception for i?)

## Visualizations

- Rework latency plot color scheme to use colors that hint at a continuum
- Adaptive temporal resolution for rate and latency plots, based on point density
- Where plots are dense, make points somewhat transparent to better show
  density?

## Web

- Use utf-8 for transferring files; I think we're doing latin-1 or ascii or
  8859-1 or something now.

## Performance

- Knossos: Identify when model/memo will be large, and don't memoize

## Core

- Deprecate model argument in checker; these should be arguments to checker
  constructors instead.
- Deprecate keyword hosts; this was a silly idea and the minor improvement
  in readability isn't really worth it.
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
