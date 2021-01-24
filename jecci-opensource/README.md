# Jecci
## What is this?
[Jepsen](https://github.com/jepsen-io/jepsen) and [Elle](https://github.com/jepsen-io/elle) based Consistency Checking Interface. You can learn more about Jepsen and Elle by their official repo. In brief, Jepsen is a test library written in clojure that can do black box tests on distributed systems and verifies whether the system behaves correctly according to different consistency models. Elle, on the other hand, is the library that does the actual checking on the history of the operations and results between jepsen and the system while testing.

However, jepsen only provides very basic framework, so if you want to test your own system, you will have to customize your own test, including how connection is setup to the system, how to translate jepsen operations to, for example, specific sql statement to some specific database. So for developers who have absolutely no knowledge on clojure, they might have to first learn clojure, then jepsen, then write their own tests.

So here comes jecci, which assumes you have absolutely no knowledge on clojure, providing only the minimal interfaces you need to implement to get the jepsen tests working, utilizing as much reusable codes as possible. And, if some new tests are added to jepsen, it will also be added here and (hopefully) is possible to used directly, providing only translations of sql statements in new test for your own database. Many of the tests here are borrowed from [TiDB](https://github.com/jepsen-io/jepsen/tree/main/tidb), and will add more later, like stolen test, etc.
## Features
### interfaces

The minimal set of functions for you to implement in order to test your cool database with a bunch of test cases immediately, almost.

See `jecci/src/jecci/interface`, what's happening here is that clojure are trying to resolve the functions or macros to your implementation, and assign the result to a variable, which will be used in tests defined in jecci/src/jecci/common. Most of them are just clients, and if not implemented, i.e. cannot be resolved, then jecci will just warn you, and prompt an Exception when running that test(if not implemented, the test will not be included when running test-all command). 

### utils

Some general functions and wrappers that can be pretty useful. 

For example, TiDB is fragile at initiating, and other dbms may have the same problem, so why don't we just reuse the retry wrappers? Also, the client implementations that take the actions from the generator and executes it should also be applicable for other dbms, so all you need to do is to provide the translation for your cool database to the client implementations and everything should work.

### tutorial

Yes of course, a very easy to understand tutorial, requiring no knowledge in clojure~
## Quick Start
First make sure you have [docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/) installed. 
Currently there is only one postgres implementation, you can also learn how to use the framework with this example. And to run the tests immediately, just do the following
```
cd jecci/docker
bash bin/up -d --dev
bash bin/console
lein run test -w bank --nemesis kill-postmaster --time-limit 60 --test-count 1 --concurrency 2n --username jecci --password 123456 --pure-generators --force-reinstall --nodes n1
```

Here `lein run test -w bank --nemesis kill-postmaster --time-limit 60 --test-count 1 --concurrency 2n --username jecci --password 123456 --pure-generators --force-reinstall --nodes n1`   is running a single test.

And to test the full suite, do `lein run test-all`

See `lein run test --help` and `lein run test-all --help` for options.
Some detailed options here:
#### Workloads (-w)
- append Checks for dependency cycles in append/read transactions
- bank concurrent transfers between rows of a shared table
- bank-multitable multi-table variant of the bank test
- long-fork distinguishes between parallel snapshot isolation and standard SI
- monotonic looks for contradictory orders over increment-only registers
- register concurrent atomic updates to a shared register
- sequential looks for serializsble yet non-sequential orders on independent registers
- set concurrent unique appends to a single table
- set-cas appends elements via compare-and-set to a single row
- table checks for a race condition in table creation
- txn-cycle looks for write-read dependency cycles over read-write registers

#### Nemeses (--nemesis)
- none no nemesis
- partition network partitions
- partition-half n/2+1 splits
- partition-one isolate single nodes
- partition-ring each node can see separate, intersecting majorities

#### Time Limit (--time-limit)
Time to run test, usually 60, 180, ... seconds
#### Test Count (--test-count)
Times to run test, should >= 1
#### Concurrency (--concurrency)
Number of threads. 2n means "twice the number of nodes", and is a good default.

# Tools
Jepsen will plot some figures for you to do analysis, and to view the plots, `feh` is recommended. If you need to get into nodes and see what's happended, do:
```
./docker/bin/console #to get into control
docker exec -it jecci-n1 bash #to get into n1, you can do the same for other nodes
```

# License
Distributed under the Eclipse Public License either version 2.0 or (at your option) any later version.
