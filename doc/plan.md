# Stuff to improve!

## Error handling

- Knossos: Better error messages when users pass models that fail on the
  first op (I think there's a ticket about this? Null pointer exception for i?)

## Visualizations

- Rework latency plot color scheme to use colors that hint at a continuum
- Adaptive temporal resolution for rate and latency plots, based on point
  density
- Nemesis regions are just lines now. Let's bring back shaded regions for :f
  :start-foo to :stop-foo? Maybe colorize for each type of nemesis f, when
  they're mixed together?
- Where plots are dense, make points somewhat transparent to better show
  density?

## Web

- Use utf-8 for transferring files; I think we're doing latin-1 or ascii or
  8859-1 or something now.

## Performance

- Knossos: we should allow users to pass a :time-limit option, and after that many seconds, abort the search. Relying on OOM detection and abort still means plenty of tests spin for hoooours.

## Core

- Deprecate model argument in checker; these should be arguments to checker
  constructors instead.
- Deprecate keyword hosts; this was a silly idea and the minor improvement
  in readability isn't really worth it.
- Clean up checker/counter: remove failed ops in an initial pre-pass, rather
  than adding them then undoing those adds.
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
