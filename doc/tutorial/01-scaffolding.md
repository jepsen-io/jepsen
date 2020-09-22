# Test scaffolding

In this tutorial, we're going to write a test for etcd: a distributed consensus
system. I want to encourage you to *type* the code yourself, even if you don't
understand everything yet. It'll help you learn faster, and you won't get as lost when we start updating small pieces of larger functions.

We'll begin by creating a new Leiningen project in any directory.

```bash
$ lein new jepsen.etcdemo
Generating a project called jepsen.etcdemo based on the 'default' template.
The default template is intended for library projects, not applications.
To see other templates (app, plugin, etc), try `lein help new`.
$ cd jepsen.etcdemo
$ ls
CHANGELOG.md  doc/  LICENSE  project.clj  README.md  resources/  src/  test/
```

Like any fresh Clojure project, we have a blank changelog, a directory for
documentation, a copy of the Eclipse Public License, a `project.clj` file,
which tells `leiningen` how to build and run our code, and a README. The
`resources` directory is a place for us to data files--for instance, config
files for a database we want to test. `src` has our source code, organized into
directories and files which match the namespace structure of our code. `test`
is for testing our code. Note that this *whole directory* is a "Jepsen test";
the `test` directory is a convention for most Clojure libraries, and we won't
be using it here.

We'll start by editing `project.clj`, which specifies the project's
dependencies and other metadata. We'll add a `:main` namespace, which is how
we'll run the test from the command line. In addition to depending on the
Clojure language itself, we'll pull in the Jepsen library, and
Verschlimmbesserung: a library for talking to etcd.

```clj
(defproject jepsen.etcdemo "0.1.0-SNAPSHOT"
  :description "A Jepsen test for etcd"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.etcdemo
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.2.1-SNAPSHOT"]
                 [verschlimmbesserung "0.1.3"]])
```

Let's try running this program with `lein run`.

```bash
$ lein run
Exception in thread "main" java.lang.Exception: Cannot find anything to run for: jepsen.etcdemo, compiling:(/tmp/form-init6673004597601163646.clj:1:73)
...
```

Ah, yes. We haven't written anything to run yet. We need a main function in the `jepsen.etcdemo` namespace, which will receive our command line args and run the test. In `src/jepsen/etcdemo.clj`:

```clj
(ns jepsen.etcdemo)

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (prn "Hello, world!" args))
```

Clojure, by default, calls our `-main` function with any arguments we passed on
the command line--whatever we type after `lein run`. It takes a variable number
of arguments (that's the `&` symbol), and calls that argument list `args`. We
print that argument list after "Hello World":

```bash
$ lein run hi there
"Hello, world!" ("hi" "there")
```

Jepsen includes some scaffolding for argument handling, running tests, handling
errors, logging, etc. Let's pull in the `jepsen.cli` namespace, call it `cli` for short, and turn our main function into a Jepsen test runner:

```clj
(ns jepsen.etcdemo
  (:require [jepsen.cli :as cli]
            [jepsen.tests :as tests]))


(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:pure-generators true}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn etcd-test})
            args))
```

`cli/single-test-cmd` is provided by `jepsen.cli`: it parses command line
arguments for a test and calls the provided `:test-fn`, which should return a
map containing all the information Jepsen needs to run a test. In this case,
our test function is `etcd-test`, which takes options from the command line
runner, and uses them to fill in position in an empty test that does nothing:
`noop-test`.

```bash
$ lein run
Usage: lein run -- COMMAND [OPTIONS ...]
Commands: test
```

With no args, `cli/run!` provides a basic help message, informing us it takes a
command as its first argument. Let's try the test command we added:

Let's give it a shot!

```bash
$ lein run test
13:04:30.927 [main] INFO  jepsen.cli - Test options:
 {:concurrency 5,
 :test-count 1,
 :time-limit 60,
 :nodes ["n1" "n2" "n3" "n4" "n5"],
 :ssh
 {:username "root",
  :password "root",
  :strict-host-key-checking false,
  :private-key-path nil}}

INFO [2018-02-02 13:04:30,994] jepsen test runner - jepsen.core Running test:
 {:concurrency 5,
 :db
 #object[jepsen.db$reify__1259 0x6dcf7b6a "jepsen.db$reify__1259@6dcf7b6a"],
 :name "noop",
 :start-time
 #object[org.joda.time.DateTime 0x79d4ff58 "2018-02-02T13:04:30.000-06:00"],
 :net
 #object[jepsen.net$reify__3493 0xae3c140 "jepsen.net$reify__3493@ae3c140"],
 :client
 #object[jepsen.client$reify__3380 0x20027c44 "jepsen.client$reify__3380@20027c44"],
 :barrier
 #object[java.util.concurrent.CyclicBarrier 0x2bf3ec4 "java.util.concurrent.CyclicBarrier@2bf3ec4"],
 :ssh
 {:username "root",
  :password "root",
  :strict-host-key-checking false,
  :private-key-path nil},
 :checker
 #object[jepsen.checker$unbridled_optimism$reify__3146 0x1410d645 "jepsen.checker$unbridled_optimism$reify__3146@1410d645"],
 :nemesis
 #object[jepsen.nemesis$reify__3574 0x4e6cbdf1 "jepsen.nemesis$reify__3574@4e6cbdf1"],
 :active-histories #<Atom@210a26b: #{}>,
 :nodes ["n1" "n2" "n3" "n4" "n5"],
 :test-count 1,
 :generator
 #object[jepsen.generator$reify__1936 0x1aac0a47 "jepsen.generator$reify__1936@1aac0a47"],
 :os
 #object[jepsen.os$reify__1176 0x438aaa9f "jepsen.os$reify__1176@438aaa9f"],
 :time-limit 60,
 :model {}}

INFO [2018-02-02 13:04:35,389] jepsen nemesis - jepsen.core Starting nemesis
INFO [2018-02-02 13:04:35,389] jepsen worker 1 - jepsen.core Starting worker 1
INFO [2018-02-02 13:04:35,389] jepsen worker 2 - jepsen.core Starting worker 2
INFO [2018-02-02 13:04:35,389] jepsen worker 0 - jepsen.core Starting worker 0
INFO [2018-02-02 13:04:35,390] jepsen worker 3 - jepsen.core Starting worker 3
INFO [2018-02-02 13:04:35,390] jepsen worker 4 - jepsen.core Starting worker 4
INFO [2018-02-02 13:04:35,391] jepsen nemesis - jepsen.core Running nemesis
INFO [2018-02-02 13:04:35,391] jepsen worker 1 - jepsen.core Running worker 1
INFO [2018-02-02 13:04:35,391] jepsen worker 2 - jepsen.core Running worker 2
INFO [2018-02-02 13:04:35,391] jepsen worker 0 - jepsen.core Running worker 0
INFO [2018-02-02 13:04:35,391] jepsen worker 3 - jepsen.core Running worker 3
INFO [2018-02-02 13:04:35,391] jepsen worker 4 - jepsen.core Running worker 4
INFO [2018-02-02 13:04:35,391] jepsen nemesis - jepsen.core Stopping nemesis
INFO [2018-02-02 13:04:35,391] jepsen worker 1 - jepsen.core Stopping worker 1
INFO [2018-02-02 13:04:35,391] jepsen worker 2 - jepsen.core Stopping worker 2
INFO [2018-02-02 13:04:35,391] jepsen worker 0 - jepsen.core Stopping worker 0
INFO [2018-02-02 13:04:35,391] jepsen worker 3 - jepsen.core Stopping worker 3
INFO [2018-02-02 13:04:35,391] jepsen worker 4 - jepsen.core Stopping worker 4
INFO [2018-02-02 13:04:35,397] jepsen test runner - jepsen.core Run complete, writing
INFO [2018-02-02 13:04:35,434] jepsen test runner - jepsen.core Analyzing
INFO [2018-02-02 13:04:35,435] jepsen test runner - jepsen.core Analysis complete
INFO [2018-02-02 13:04:35,438] jepsen results - jepsen.store Wrote /home/aphyr/jepsen/jepsen.etcdemo/store/noop/20180202T130430.000-0600/results.edn
INFO [2018-02-02 13:04:35,440] main - jepsen.core {:valid? true}

Everything looks good! ヽ(‘ー`)ノ
```

We can see Jepsen start a series of workers--each one responsible for executing
operations against the database--and a nemesis, which causes failures. We
haven't given them anything to do, so they shut down immediately. Jepsen writes
out the result of this (trivial) test to the `store` directory, and prints out
a brief analysis.

`noop-test` uses nodes named `n1`, `n2`, ... `n5` by default. If your nodes
have different names, this test will fail to connect to them. That's OK! You can change that by passing node names on the command line:

```bash
$ lein run test --node foo.mycluster --node 1.2.3.4
```

... or by passing a filename that has a list of nodes in it, one per line. If
you're using the AWS Marketplace cluster, you've already got a file called
`nodes` in your home directory, ready to go.

```bash
$ lein run test --nodes-file ~/nodes
```

If you're still hitting SSH errors at this point, you should check that your
SSH agent is running and has keys for all your nodes loaded. `ssh some-db-node`
should work without a password. You can override the username, password, and
identity file at the command line as well; see `lein run test --help` for
details.

```bash
$ lein run test --help
#object[jepsen.cli$test_usage 0x7ddd84b5 jepsen.cli$test_usage@7ddd84b5]

  -h, --help                                                  Print out this message and exit
  -n, --node HOSTNAME             ["n1" "n2" "n3" "n4" "n5"]  Node(s) to run test on
      --nodes-file FILENAME                                   File containing node hostnames, one per line.
      --username USER             root                        Username for logins
      --password PASS             root                        Password for sudo access
      --strict-host-key-checking                              Whether to check host keys
      --ssh-private-key FILE                                  Path to an SSH identity file
      --concurrency NUMBER        1n                          How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes.
      --test-count NUMBER         1                           How many times should we repeat a test?
      --time-limit SECONDS        60                          Excluding setup and teardown, how long should a test run for, in seconds?
```

We'll use `lein run test ...` throughout this guide to re-run our Jepsen test. Each time we run a test, Jepsen will create a new directory in `store/`. You can see the latest results in `store/latest`:

```bash
$ ls store/latest/
history.txt  jepsen.log  results.edn  test.fressian
```

`history.txt` shows the operations the test performed--ours is empty, since the
noop test doesn't perform any ops. `jepsen.log` has a copy of the console log
for that test. `results.edn` shows the analysis of the test, which we see at
the end of each run. Finally, `test.fressian` has the raw data for the test,
including the full machine-readable history and analysis, if you need to
perform post-hoc analysis.

Jepsen also comes with a built-in web browser for browsing these results. Let's add it to our `main` function:

```clj
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn etcd-test})
                   (cli/serve-cmd))
            args))
```

This works because `cli/run!` takes a map of command names to specifications of
how to run those commands. We're merging those maps together with `merge`.

```bash
$ lein run serve
13:29:21.425 [main] INFO  jepsen.web - Web server running.
13:29:21.428 [main] INFO  jepsen.cli - Listening on http://0.0.0.0:8080/
```

We can open `http://localhost:8080` in a web browser to explore the history of
our test results. Of course, the serve command comes with its own options and
help message:

```bash
$ lein run serve --help
Usage: lein run -- serve [OPTIONS ...]

  -h, --help                  Print out this message and exit
  -b, --host HOST    0.0.0.0  Hostname to bind to
  -p, --port NUMBER  8080     Port number to bind to
```

Open up a new terminal window, and leave the web server running there. That way
we can see the results of our tests without having to start and stop it
repeatedly.

With this groundwork in place, we'll write the code to [set up and tear down the database](02-db.md).
