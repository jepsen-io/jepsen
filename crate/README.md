# jepsen.crate

A Clojure library designed to test CrateDB for the following issues that can
occur because of network partitioning:

 - Data divergence between the nodes
 - Dirty reads
 - Lost updates

You can find detailed information on those three issues [here](https://github.com/elastic/elasticsearch/issues/20031).

## Usage

For convenience and flexibility we have chosen to run the Jepsen tests using
docker containers.

#### Prepare Docker Containers

Make sure you have docker installed and the daemon running and the execute:

    $ cd docker && ./up.sh


#### Run All Tests

Once the docker containers are up and running from another terminal execute:

    $ docker exec -it jepsen-control bash

Then inside the ``jepsen-control`` docker container run:

    $ cd crate && ./runall.sh [CrateDB tarball URL]

``CrateDB tarball URL]`` can be for example: ``https://cdn.crate.io/downloads/releases/crate-2.1.8.tar.gz``

We also have a test defined that runs dirty-read several times and mixes up
the operations (some against ``CrateDB`` and some against
``Elasticsearch``). This test scenario can be run using `lein test` in the
control box/container (it points at the latest nightly build).

#### Run Individual Tests

If instead of running all the tests at once you want to run one them then
execute:

    $ cd crate/scripts && ./[TestScenario].sh [CrateDB tarball URL]

``TestScenario`` can be one of the:

 - ``dirty-read``
 - ``lost-updates``
 - ``version-divergence``


#### Test Results

After the test scenarios have run you can see the results inside the
``/jepsen/crate/store`` directory. For each test there is a directory and there
is also a symlink ``latest`` pointing to the latest test that run.

Inside each test directory you can find directories for each test scenario
one directory for each run of the specific test marked with a timestamp. For
example: ``20171018T150316.000Z`` and also a symlink ``latest`` that points to
the latest run of the test.

Inside each individual run of a test you can find the log files and results of
the test run. More specifically you can see:

 - ``jepsen.log`` contains logging information of the test run.
 - ``timeline.txt`` graphical timeline representation of the events that took
   place.
 - ``latency-quantiles.png``, ``latency-raw.png``, ``rate.png`` various graphs
   related to the events that took place during the test run.
 - ``n[1-5]`` a directory for every CrateDB node configured for the test, each
   one containing the ``crate.log`` file with logging information for that
   node.

You can copy the store directory (or parts of it) from the docker container to
the docker host machine by executing:

    $ docker cp jepsen-control:/jepsen/crate/store [DST_DIR_NAME]

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
