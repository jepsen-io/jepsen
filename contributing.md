# Contributing to Jepsen

Hi there, and thanks for helping make Jepsen better! I've got just one request:
start your commit messages with the *part* of Jepsen you're changing. For
instance, if I made a change to the MongoDB causal consistency tests:

> MongoDB causal: fix a bug when analyzing zero-length histories

Namespaces are cool too!

> jepsen.os.debian: fix libzip package name for debian stretch

If you're making a chance to the core Jepsen library, as opposed to a specific
database test, you can be more concise:

> add test for single nemesis events

Jepsen's a big project with lots of moving parts, and it can be confusing to
read the commit logs. Giving a bit of context makes my life a lot easier.

Thanks!
