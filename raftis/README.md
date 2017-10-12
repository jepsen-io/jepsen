# Raftis Jepsen Test

A Clojure library designed to test [Raftis](https://github.com/Qihoo360/floyd/tree/master/floyd/example/redis), a Redis protocol raft cluster implemented by [Floyd](https://github.com/Qihoo360/floyd).

## What is Raftis

Raftis = Raft + Redis protocol

Raftis support redis's kv interface for now. We implement the redis protocol consistent system with Floyd, a library that could be easily embeded into users' application. Raftis is just like zookeeper, etcd.


### Run Tests

Add hostname `n1 n2 n3 n4 n5` to `/etc/hosts`

`lein run test --help`

`lein run test --time-limit 40 --concurrency 10 --test-count 10`

## License

Copyright © 2017 Qihoo360

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
