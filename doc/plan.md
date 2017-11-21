# Stuff to improve!

## Error handling

- [x] When tracing SSH commands in jepsen.control, log the node name too
- [x] When calling methods in jepsen.control, it'd be nice if thrown
  com.jcraft.jsch.JSchException's would also log debugging info about the node,
  username, password, etc.
- [x] Better error on gnuplot missing
- [ ] Knossos: Better error messages when users pass models that fail on the
  first op (I think there's a ticket about this? Null pointer exception for i?)

## Visualizations

- [x] When timeline bars are tall, it'd be nice to see the full operation on
  hover.
- [x] Timeline bars should show wall-clock times as well--derived from op :time
  in nanoseconds, plus test start time.
- [x] You should be able to link into a specific op in the timeline
- [ ] Rework latency plot color scheme to use colors that hint at a continuum
- [ ] Adaptive temporal resolution for rate and latency plots, based on point density
- [ ] Where plots are dense, make points somewhat transparent to better show
  density?
- [ ] Latency plots: layer least frequent events on top of more frequent ones

## Web

- [ ] Use utf-8 for transferring files; I think we're doing latin-1 or ascii or
  8859-1 or something now.

## Performance

- [x] Cache index page of web server
- [ ] Knossos: Identify when model/memo will be large, and don't memoize

## Core

- [ ] Deprecate model argument in checker; these should be arguments to checker
  constructors instead.
- [ ] Deprecate keyword hosts; this was a silly idea and the minor improvement
  in readability isn't really worth it.
- [ ] Macro like (synchronize-nodes test), which enforces a synchronization
  barrier where (count nodes threads) must come to sync on the test map.
- [x] jepsen.control/upload should take java.io.Files as well as strings, and
  use .getCanonicalPath to figure out what to upload. Maybe return remote path?
- [x] Extract jepsen.model models and move them into knossos.model; delete
  jepsen.model.

## Extensions

- [ ] Reusable packet capture utility (take from cockroach)

## New tests

- [ ] Port bank test from Galera into core (alongside G2)
- [ ] Port query-across-tables-and-insert from Cockroach into core
- [ ] Port pure-insert from Cockroach into core
- [ ] Port comments from Cockroach into core (better name?)
- [ ] Port other Hermitage tests to Jepsen?
