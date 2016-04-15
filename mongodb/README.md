# Jepsen MongoDB tests

Evaluates single-document compare-and-set against a MongoDB cluster.

## Examples

```sh
# Short test with write and read concern majority
lein run

# 100 second test with write concern "journaled" and "local" read concern
lein run -t 100 -w journaled -r local

# Use the mmapv1 storage engine
lein run -s mmapv1

# Pick a different tarball to install
lein run --tarball https://...foo.tar.gz
```

## Building and running as a single jar

```sh
lein uberjar
java -jar target/jepsen.mongodb-0.2.0-SNAPSHOT-standalone.jar -t 500 ...
```

## Full usage

```
$ lein run -- -h
Usage: java -jar jepsen.mongodb.jar [OPTIONS ...]

Runs a Jepsen test and exits with a status code:

  0     All tests passed
  1     Some test failed
  254   Invalid arguments
  255   Internal Jepsen error

Options:

  -h, --help                                                                                             Print out this message and exit
  -t, --time-limit SECONDS     150                                                                       Excluding setup and teardown, how long should tests run for?
  -w, --write-concern LEVEL    :majority                                                                 Write concern level
  -r, --read-concern LEVEL     :majority                                                                 Read concern level
  -s, --storage-engine ENGINE  wiredTiger                                                                Mongod storage engine
  -p, --protocol-version INT   1                                                                         Replication protocol version number
      --tarball URL            https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-debian71-3.3.1.tgz  URL of the Mongo tarball to install
```

## License

Copyright © 2015, 2016 Kyle Kingsbury & Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
