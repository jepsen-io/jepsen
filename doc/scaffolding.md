# Test scaffolding

Let's say we want to write a test for the Zookeeper configuration store. We'll
begin by creating a new Leiningen project:

```bash
$ lein new jepsen.zookeeper
Generating a project called jepsen.zookeeper based on the 'default' template.
The default template is intended for library projects, not applications.
To see other templates (app, plugin, etc), try `lein help new`.
$ cd jepsen.zookeeper
```

We'll need a few Clojure libraries for this test. Open `project.clj`, which
specifies the project's dependencies and other metadata. In addition to
depending on the Clojure language itself, we'll pull in the Jepsen library (at
version 0.0.9), and Avout: a library for working with Zookeeper.

```clj
(defproject jepsen.zookeeper "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [jepsen "0.0.9"]
                 [avout "0.5.4"]])
```

New lein projects include a trivial failing test, which we can run with `lein
test`:

```bash
$ lein test

lein test jepsen.zookeeper-test

lein test :only jepsen.zookeeper-test/a-test

FAIL in (a-test) (zookeeper_test.clj:7)
FIXME, I fail.
expected: (= 0 1)
  actual: (not (= 0 1))

Ran 1 tests containing 1 assertions.
1 failures, 0 errors.
Tests failed.
```

We'll use `lein test` throughout this guide to re-run our Jepsen test. Let's
start with a simple test that does nothing. In `src/jepsen/zookeeper.clj`,
we'll require the `jepsen.tests` namespace and call it `tests` for short. Then
we'll write a function `zk-test` which takes a zookeeper version string as an
argument, ignores it, and returns a Jepsen test that does nothing. We'll use
this `noop-test` as a scaffold, gradually hooking in more pieces as we build
them.

```clj
(ns jepsen.zookeeper
  (:require [jepsen.tests :as tests]))

(defn zk-test
  [version]
  tests/noop-test)
```

`tests/noop-test` refers to the `noop-test` var in the `jepsen.tests`
namespace--which we aliased to `tests` in the `:require` section of our
namespace declaration.

Next, we'll replace the example test that lein generated
(`test/jepsen/zookeeper_test.clj`) with one that calls the `zk-test` function,
runs the test that function returns, looks at the results, and ensures that the
`:valid?` key is true.

```clj
(ns jepsen.zookeeper-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.zookeeper :as zk]))

(deftest zk-test
  (is (:valid? (:results (jepsen/run! (zk/zk-test "3.4.5+dfsg-2"))))))
```

We can run this test to confirm that everything's hooked up correctly:

```bash
$ lein test
WARNING: run! already refers to: #'clojure.core/run! in namespace: jepsen.core, being replaced by: #'jepsen.core/run!
WARNING: run! already refers to: #'clojure.core/run! in namespace: jepsen.tests, being replaced by: #'jepsen.core/run!

lein test jepsen.zookeeper-test
INFO  jepsen.core - nemesis done
INFO  jepsen.core - Worker 0 starting
INFO  jepsen.core - Worker 2 starting
INFO  jepsen.core - Worker 0 done
INFO  jepsen.core - Worker 2 done
INFO  jepsen.core - Worker 1 starting
INFO  jepsen.core - Worker 4 starting
INFO  jepsen.core - Worker 3 starting
INFO  jepsen.core - Worker 1 done
INFO  jepsen.core - Worker 4 done
INFO  jepsen.core - Worker 3 done
INFO  jepsen.core - Waiting for nemesis to complete
INFO  jepsen.core - nemesis done.
INFO  jepsen.core - Tearing down nemesis
INFO  jepsen.core - Nemesis torn down
INFO  jepsen.core - Run complete, writing
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.zookeeper/store/noop/20151231T155934.000-0800/history.txt
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.zookeeper/store/noop/20151231T155934.000-0800/results.edn
INFO  jepsen.core - Analyzing
INFO  jepsen.core - Analysis complete
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.zookeeper/store/noop/20151231T155934.000-0800/history.txt
INFO  jepsen.store - Wrote /home/aphyr/jepsen/jepsen.zookeeper/store/noop/20151231T155934.000-0800/results.edn
INFO  jepsen.core - Everything looks good! ヽ(‘ー`)ノ

{:valid? true, :configs ({:model {}, :pending []}), :final-paths ()}


Ran 1 tests containing 1 assertions.
0 failures, 0 errors.
```

We can see Jepsen start a series of workers--each one responsible for executing
operations against the database--and a nemesis, which causes failures. We
haven't given them anything to do, so they shut down immediately. Jepsen spits
out a text file describing the clients' and nemesis' actions to `history.txt`,
and a full dump of the test results to `results.edn` in the `store` directory. Note that the path includes the name of the test (`noop`), and a timestamp. Jepsen also constructs a `latest` symlink at each level, so we can quickly inspect the most recent test results.

```bash
$ cat store/latest/results.edn
{:valid? true, :configs ({:model {}, :pending []}), :final-paths ()}
```

With the groundwork in place, we'll write the code to [set up and tear down the database](db.md)
