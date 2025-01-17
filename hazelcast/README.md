# jepsen.hazelcast

A Jepsen test suite for Hazelcast IMDG.

## How to Use

We run the Jepsen tests using docker containers for convenience.

First, make sure that Docker is installed. To start Jepsen containers, execute the following command in `jepsen`
directory which is one level up from `hazelcast` directory:

    $ cd docker && ./up.sh

Our Jepsen tests are ready to go once all containers are up and running fine. You can get into the `jepsen-control`
container by executing the following command:

    $ docker exec -it jepsen-control bash

Then, while you are inside the `jepsen-control` container, run the following command:

    $ cd hazelcast

While you are at the `hazelcast` directory, you can run our Jepsen tests using `lein`. For instance,
`lein run test --workload non-reentrant-cp-lock --time-limit 120` runs the non-reentrant lock test for 120 seconds.

You can also use the `repeat` scripts to run tests multiple times. For instance, while in the `hazelcast` directory,
run `sh repeat_single_test.sh non-reentrant-cp-lock 5 120` to run the non-reentrant CP lock test 5 times, each test taking 120
seconds, or run `sh repeat_all_cp_tests.sh 5 120` to run the whole CP subsystem test suite 5 times, each test taking 120 seconds.
These scripts stop on a test failure so that you can report and investigate the failure.

## Test Cases

With Hazelcast version 3.12, we released our Jepsen test suite for the CP data structures. Besides the new CP subsystem
test suite, there are other tests written by Kyle Kingsbury to test AP data structures in Hazelcast.

Our CP subsystem test suite is as follows:
- Non-reentrant lock (`--workload non-reentrant-cp-lock`): In this test case, we test if the new `FencedLock`
data structure behaves as a non-reentrant mutex, i.e., it can be held by a single endpoint at a time and only the lock
holder endpoint can release it. Moreover, the lock cannot be acquired by the same endpoint reentrantly. It means that
while a client holds the lock, it cannot acquire the lock again, without releasing the lock first.

- Reentrant lock (`--workload reentrant-cp-lock`): We test if the new `FencedLock` data structure behaves as a reentrant
mutex. The lock instance can be held by a single endpoint at a time and only the lock holder endpoint can release it.
Moreover, the current lock holder can reentrantly acquire the lock one more time. Reentrant lock acquire limit is 2 for
this test.

- Non-reentrant fenced lock (`--workload non-reentrant-fenced-lock`): `FencedLock` orders lock holders by a monotonic
fencing token, which is incremented each time the lock switches from the free state to the held state. In this test
case, we validate monotonicity of fencing tokens assigned to subsequent lock holders. Moreover, the lock cannot be
acquired by the same endpoint reentrantly. It means that while a client holds the lock, it cannot acquire the lock
again, without releasing the lock first.

- Reentrant fenced lock (`--workload reentrant-fenced-lock`): `FencedLock` orders lock holders by a monotonic fencing
token, which is incremented each time the lock switches from the free state to the held state. However, if the current
lock holder acquires the lock reentrantly, it will get the same fencing token. Reentrant lock acquire limit is 2 for
this test.

- Semaphore (`--workload semaphore`): In this test, we initialize our new linearizable `ISemaphore` with 2 permits. Each
client acquires and releases a permit in a loop and we validate permits are held by at most 2 clients at a time.

- Unique ID Generation with the new linearizable `IAtomicLong` (`--workload cp-id-gen-long`): In this test,
each client generates a unique long id by using a linearizable `IAtomicLong` instance and we validate uniqueness of
generated ids.

- Compare-and-swap Register with the new linearizable `IAtomicLong` (`--workload cp-cas-long`): In this test,
clients randomly perform write and compare-and-swap operations.

- Compare-and-swap Register with the new linearizable `IAtomicReference` (`--workload cp-cas-reference`): In this test,
clients randomly perform write and compare-and-swap operations.

In each test, multiple clients send concurrent operations to a shared data structure, which is replicated
to the Hazelcast cluster. In the mean time, Jepsen's `nemesis` injects network partitions into the system. At the end
of the test, Jepsen validates if the history of operations is linearizable. We are planning to add more failure modes
in future.

## License

Original work Copyright © 2015-2019, Jepsen, LLC

Modified work Copyright © 2019, Hazelcast, Inc. All Rights Reserved.

Distributed under the Eclipse Public License version 1.0 or (at your option) any later version.
