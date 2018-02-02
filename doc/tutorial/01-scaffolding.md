# Test scaffolding

Let's say we want to write a test for etcd: a distributed consensus
system. We'll begin by creating a new Leiningen project in any directory.

```bash
$ lein new jepsen.etcdemo
Generating a project called jepsen.etcdemo based on the 'default' template.
The default template is intended for library projects, not applications.
To see other templates (app, plugin, etc), try `lein help new`.
$ cd jepsen.etcdemo
```

We'll need a few Clojure libraries for this test. Open `project.clj`, which
specifies the project's dependencies and other metadata. We'll add a `:main`
namespace, which is how we'll run the test from the command line. In addition
to depending on the Clojure language itself, we'll pull in the Jepsen library
(at version 0.1.4), and Verschlimmbesserung: a library for talking to etcd.

```clj
(defproject jepsen.etcdemo "0.1.0-SNAPSHOT"
  :description "A Jepsen test for etcd"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.etcdemo
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.4"]
                 [verschlimmbesserung "0.1.3"]])
```

Let's try running this program with `lein run` (after running `lein deps` to get dependencies - just the first time).

```bash
$ lein run
Exception in thread "main" java.lang.Exception: Cannot find anything to run for: jepsen.etcdemo, compiling:(/tmp/form-init6673004597601163646.clj:1:73)
...
```

Ah, yes. We haven't written anything to run yet. We need a main function in the `jepsen.etcdemo` namespace, which will receive our command line args and run the test. In `src/jepsen/etcdemo.clj`:

```clj
(ns jepsen.etcdemo
  (:gen-class))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (prn "Hello, world!" args))
```

`:gen-class` is a bit of Clojure plumbing; it tells Clojure that we intend to
run this namespace from the command line. Our `-main` function takes a variable
number of arguments (`args`), and prints them out after "Hello World":

```bash
$ lein run hi there
"Hello, world!" ("hi" "there")
```

Jepsen includes some scaffolding for argument handling, running tests, handling
errors, logging, etc. Let's pull in the `jepsen.cli` namespace, call it `cli` for short, and turn our main function into a Jepsen test runner:

```clj
(ns jepsen.etcdemo
  (:gen-class)
  (:require [jepsen.cli :as cli]
            [jepsen.tests :as tests]))


(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
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
09:50:17.038 [main] INFO  jepsen.cli - Test options:
 {:concurrency 5,
 :test-count 1,
 :time-limit 60,
 :nodes [:n1 :n2 :n3 :n4 :n5],
 :ssh
 {:username "root",
  :password "root",
  :strict-host-key-checking false,
  :private-key-path nil}}

INFO [2017-03-30 09:50:20,395] jepsen nemesis - jepsen.core Nemesis starting
INFO [2017-03-30 09:50:20,396] jepsen nemesis - jepsen.core nemesis done
INFO [2017-03-30 09:50:20,398] jepsen worker 0 - jepsen.core Worker 0 starting
INFO [2017-03-30 09:50:20,398] jepsen worker 4 - jepsen.core Worker 4 starting
INFO [2017-03-30 09:50:20,398] jepsen worker 3 - jepsen.core Worker 3 starting
INFO [2017-03-30 09:50:20,398] jepsen worker 1 - jepsen.core Worker 1 starting
INFO [2017-03-30 09:50:20,398] jepsen worker 2 - jepsen.core Worker 2 starting
INFO [2017-03-30 09:50:20,398] jepsen worker 0 - jepsen.core Worker 0 done
INFO [2017-03-30 09:50:20,398] jepsen worker 4 - jepsen.core Worker 4 done
INFO [2017-03-30 09:50:20,398] jepsen worker 3 - jepsen.core Worker 3 done
INFO [2017-03-30 09:50:20,398] jepsen worker 1 - jepsen.core Worker 1 done
INFO [2017-03-30 09:50:20,398] jepsen worker 2 - jepsen.core Worker 2 done
INFO [2017-03-30 09:50:20,398] jepsen test runner - jepsen.core Waiting for nemesis to complete
INFO [2017-03-30 09:50:20,399] jepsen test runner - jepsen.core nemesis done.
INFO [2017-03-30 09:50:20,399] jepsen test runner - jepsen.core Tearing down nemesis
INFO [2017-03-30 09:50:20,399] jepsen test runner - jepsen.core Nemesis torn down
INFO [2017-03-30 09:50:20,401] jepsen test runner - jepsen.core Run complete, writing
INFO [2017-03-30 09:50:20,443] jepsen test runner - jepsen.core Analyzing
INFO [2017-03-30 09:50:20,457] jepsen test runner - jepsen.core Analysis complete
INFO [2017-03-30 09:50:20,466] jepsen results - jepsen.store Wrote /home/aphyr/jepsen/etcdemo/store/noop/20170330T095017.000-0500/results.edn
INFO [2017-03-30 09:50:20,472] main - jepsen.core {:valid? true,
 :configs ({:model {}, :last-op nil, :pending []}),
 :final-paths ()}


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

  -h, --help                                             Print out this message and exit
  -n, --node HOSTNAME             [:n1 :n2 :n3 :n4 :n5]  Node(s) to run test on
      --nodes-file FILENAME                              File containing node hostnames, one per line.
      --username USER             root                   Username for logins
      --password PASS             root                   Password for sudo access
      --strict-host-key-checking                         Whether to check host keys
      --ssh-private-key FILE                             Path to an SSH identity file
      --concurrency NUMBER        1n                     How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes.
      --test-count NUMBER         1                      How many times should we repeat a test?
      --time-limit SECONDS        60                     Excluding setup and teardown, how long should a test run for, in seconds?
```

We'll use `lein run` throughout this guide to re-run our Jepsen test. Each time we run a test, Jepsen will create a new directory in `store/`. You can see the latest results in `store/latest`:

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

With this groundwork in place, we'll write the code to [set up and tear down the database](db.md)
