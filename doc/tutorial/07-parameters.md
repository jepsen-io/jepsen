# Tuning with Parameters

We allowed our last test to pass by including a `quorum` flag on reads, but in order to see the original stale-reads bug, we have to edit the source code again, flipping the flag to `false`. It'd be nice if we could adjust that from the command line. Jepsen provides several command-line options by default in [jepsen.cli](https://github.com/jepsen-io/jepsen/blob/0.1.7/jepsen/src/jepsen/cli.clj#L52-L87), but we can add our own options by passing an `:opt-spec` to `cli/single-test-cmd`.

```clj
(def cli-opts
  "Additional command line options."
    [["-q" "--quorum" "Use quorum reads, instead of reading from any primary."]])
```

CLI options are a collection of vectors, giving a short name, a full name, a
documentation string, and options which affect how that option is parsed, its
default value, etc. These are passed to
[tools.cli](https://github.com/clojure/tools.cli), the standard Clojure library
for option handling.

Now, let's pass that option specification to the CLI:

```clj
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  etcd-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args)
```

If we re-run our test with `lein run test -q ...`, we'll see a new `:quorum` option in our test map:

```clj
10:02:42.532 [main] INFO  jepsen.cli - Test options:
 {:concurrency 10,
 :test-count 1,
 :time-limit 30,
 :quorum true,
 ...
```

Jepsen parsed our `-q` option, found the option specification we provided, and
added a `:quorum true` pair to the options map. That options map was passed to
`etcd-test`, which `merge`d it into the test map. Viola! We have a `:quorum`
key in our test!

Now, let's use that quorum option to control whether the client issues quorum
reads, in the Client `invoke` function:

```clj
        (case (:f op)
          :read (let [value (-> conn
                                (v/get k {:quorum? (:quorum test)})
                                parse-long-nil)]
                  (assoc op :type :ok, :value (independent/tuple k value)))
```

Let's try `lein run` with and without quorum reads, and see whether it lets us
see the stale reads bug again.

```bash
$ lein run test -q ...
...

$ lein run test ...
...
clojure.lang.ExceptionInfo: throw+: {:errorCode 209, :message "Invalid field", :cause "invalid value for \"quorum\"", :index 0, :status 400}
...
```

Huh. Let's double-check what the value was for `:quorum` in the test map. It's logged at the beginning of every Jepsen run:

```clj
2018-02-04 09:53:24,867{GMT}	INFO	[jepsen test runner] jepsen.core: Running test:
 {:concurrency 10,
 :db
 #object[jepsen.etcdemo$db$reify__4946 0x15a8bbe5 "jepsen.etcdemo$db$reify__4946@15a8bbe5"],
 :name "etcd",
 :start-time
 #object[org.joda.time.DateTime 0x54a5799f "2018-02-04T09:53:24.000-06:00"],
 :net
 #object[jepsen.net$reify__3493 0x2a2b3aff "jepsen.net$reify__3493@2a2b3aff"],
 :client {:conn nil},
 :barrier
 #object[java.util.concurrent.CyclicBarrier 0x6987b74e "java.util.concurrent.CyclicBarrier@6987b74e"],
 :ssh
 {:username "root",
  :password "root",
  :strict-host-key-checking false,
  :private-key-path nil},
 :checker
 #object[jepsen.checker$compose$reify__3220 0x71098fb3 "jepsen.checker$compose$reify__3220@71098fb3"],
 :nemesis
 #object[jepsen.nemesis$partitioner$reify__3601 0x47c15468 "jepsen.nemesis$partitioner$reify__3601@47c15468"],
 :active-histories #<Atom@18bf1bad: #{}>,
 :nodes ["n1" "n2" "n3" "n4" "n5"],
 :test-count 1,
 :generator
 #object[jepsen.generator$time_limit$reify__1996 0x483fe83a "jepsen.generator$time_limit$reify__1996@483fe83a"],
 :os
 #object[jepsen.os.debian$reify__2908 0x8aa1562 "jepsen.os.debian$reify__2908@8aa1562"],
 :time-limit 30,
 :model {:value nil}}
```

Oh. That's odd. There... *isn't* a `:quorum` key here. Option flags only appear
in the options map if they're present on the command line; if they're left out
of the command line, they're left out of the option map too. When we ask for
`(:quorum test)`, and `test` *has* no `:quorum` option, we'll get `nil`.

There are a few easy ways to fix this. We could coerce `nil` to `false` by
using `(boolean (:quorum test))`, at the client, or in `etcd-test`. Or we could
force the opt spec to provide a default value when the flag is omitted, by
adding `:default false` to the quorum opt-spec. We'll apply `boolean` in
`etcd-test`, just in case someone calls it directly, instead of through the
CLI.

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map. Special options:

      :quorum     Whether to use quorum reads"
  [opts]
  (let [quorum (boolean (:quorum opts))]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name            (str "etcd q=" quorum)
            :quorum          quorum

            ...
```

We're binding `quorum` to a variable here so that we can use its boolean value
in two places. We add it to the test's `name`, which makes it easy to tell
which tests used quorum reads at a glance. We also add it to the `:quorum`
option. Since we merge `opts` *before* that, our boolean version of `:quorum`
will take precedence over whatever in `opts`. Now, without `-q`, our test can
find errors again:

```bash
$ lein run test --time-limit 60 --concurrency 100 -q
...
Everything looks good! ヽ(‘ー`)ノ

$ lein run test --time-limit 60 --concurrency 100
...
Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
```

## Tunable difficulty

Depending on how powerful your computer is, you may have noticed some tests get
stuck on painfully slow analyses. It's hard to control this up-front--the
difficulty of a test goes like `~n!`, where n is the number of concurrent
processes. A couple crashed processes can make the difference between seconds
and days to check.

To help with this problem, let's add some tuning options to our test which
control the number of operations you can perform on any single key, and how
fast operations are generated.

In the generator, let's change our hardcoded 1/10 delay to a parameter, given
as a rate per second, and change our hardcoded limit on each key's generator to a configurable parameter.

```clj
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency ...), constructs a test map."
  [opts]
  (let [quorum (boolean (:quorum opts))]
    (merge tests/noop-test
           opts
           {:pure-generators true
            :name            (str "etcd q=" quorum)
            :quorum          quorum
            :os              debian/os
            :db              (db "v3.1.5")
            :client          (Client. nil)
            :nemesis         (nemesis/partition-random-halves)
            :checker         (checker/compose
                               {:perf   (checker/perf)
                                :indep (independent/checker
                                         (checker/compose
                                           {:linear   (checker/linearizable
                                                        {:model (model/cas-register)
                                                         :algorithm :linear})
                                            :timeline (timeline/html)}))})
            :generator       (->> (independent/concurrent-generator
                                    10
                                    (range)
                                    (fn [k]
                                      (->> (gen/mix [r w cas])
                                           (gen/stagger (/ (:rate opts)))
                                           (gen/limit (:ops-per-key opts)))))
                                  (gen/nemesis
                                    (->> [(gen/sleep 5)
                                          {:type :info, :f :start}
                                          (gen/sleep 5)
                                          {:type :info, :f :stop}]
                                         cycle))
                                  (gen/time-limit (:time-limit opts)))})))
```

And add corresponding command-line options

```clj
(def cli-opts
  "Additional command line options."
  [["-q" "--quorum" "Use quorum reads, instead of reading from any primary."]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])
```

We don't have to provide a short name for every option: we use `nil` to
indicate that `--ops-per-key` has no short form. The capital words after each
flag (e.g. "HZ" & "NUM") are arbitrary placeholders for values that you would
pass. They'll be printed as a part of the usage documentation. We provide a
`:default` for both options, which is used if there's no flag at the command
line. For rates, we want to allow integers, decimals, and fractions, so...
we'll use Clojure's built-in `read-string` function to parse all three. Then
we'll validate that it's both a number and that it's positive, to keep people
from passing strings, negative numbers, zero rates, etc.

Now, if we want to run a less aggressive test, we can try

```bash
$ lein run test --time-limit 10 --concurrency 10 --ops-per-key 10 -r 1
...
Everything looks good! ヽ(‘ー`)ノ
```

Looking through the history for each key, we can see that operations proceeded
very slowly, and there are only 10 per key. This test is much easier to check!
However, it also fails to find the bug! This is an inherent tension in Jepsen:
we have to be aggressive to find errors, but verifying those aggressive
histories can be *much* more difficult--even impossible.

Linearizability checking is NP-hard; there's no way around that. We can design
somewhat more efficient checkers, but eventually, that exponential cliff is
going to bite us. Perhaps, however... we could verify a *weaker* property.
Something in linear or logarithmic time. Let's [add a commutative
test](08-set.md)
