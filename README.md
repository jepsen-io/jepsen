# Jepsen

Breaking distributed systems so you don't have to. I'm tearing apart Jepsen as
I work on the next iteration of the talk, so things are a bit messy. Pardon my
dust. :(

If you came here looking for the tests from the first two jepsen talks, or for
the foundationDB tests, you'll find them in the `old` branch. I've gutted the
test rig for those systems and replaced it with something a lot faster and more
powerful; if you want to help port the old tests forward, I could really use
the help!

To get started, you'll need five debian boxes (I run debian testing, but some
DBs don't need the latest packages so you might get away with an older
distribution, or possibly ubuntu). I run em in LXC containers. Each one should
be accessible via SSH. By default they're named n1, n2, n3, n4, and n5, but
that (along with SSH username, password, identity files, etc) is all definable
in your test. The account you use on those boxes needs sudo access to set up
DBs and run firewalls. Be advised that it's gonna run killall -9 on some
processes, so you shouldn't, you know, point jepsen at your prod machines.

Your local machine needs a JVM and leiningen installed.

For an overview of how a database test works, see
`src/jepsen/system/elasticsearch.clj` and its corresponding test
`test/jepsen/system/elasticsearch-test.clj`, which you can invoke via `lein run
jepsen.system.elasticsearc-test`.
