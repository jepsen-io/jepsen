# Stuff to improve!

## Error handling

- [ ] When calling methods in jepsen.control, it'd be nice if thrown
  com.jcraft.jsch.JSchException's would also log debugging info about the node,
  username, password, etc.
- [ ] Better error on gnuplot missing
- [ ] Knossos: Better error messages when users pass models that fail on the
  first op (I think there's a ticket about this? Null pointer exception for i?)

## Visualizations

- [ ] When timeline bars are tall, it'd be nice to see the full operation on
  hover.
- [ ] Timeline bars should show wall-clock times as well--derived from op :time
  in nanoseconds, plus test start time.
- [ ] You should be able to link into a specific op in the timeline
- [ ] Rework latency plot color scheme to use colors that hint at a continuum
- [ ] Adaptive temporal resolution for rate and latency plots, based on point density
- [ ] Where plots are dense, make points somewhat transparent to better show
  density?
- [ ] Latency plots: layer least frequent events on top of more frequent ones

## Performance

- [ ] Cache index page of web server
- [ ] Knossos: Identify when model/memo will be large, and don't memoize

## Core

- [ ] Deprecate model argument in checker; these should be arguments to checker
  constructors instead.

## Extensions

- [ ] Reusable packet capture utility (take from cockroach)

## New tests

- [ ] Port bank test from Galera into core (alongside G2)
- [ ] Port query-across-tables-and-insert from Cockroach into core
- [ ] Port pure-insert from Cockroach into core
- [ ] Port comments from Cockroach into core (better name?)
- [ ] Port other Hermitage tests to Jepsen?
