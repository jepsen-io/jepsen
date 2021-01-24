# Dockerized Jecci

This docker image attempts to simplify the setup required by Jecci.
It is intended to be used by a CI tool or anyone with Docker who wants to try Jecci themselves.

It contains all the jecci dependencies and code. It uses [Docker
Compose](https://github.com/docker/compose) to spin up the five containers used
by Jecci.

## Quickstart

Assuming you have docker-compose set up already, run:

```
bin/up
bin/console
```

... which will drop you into a console on the Jecci control node.

Your DB nodes are `n1`, `n2`, `n3`, `n4`, and `n5`. You can open as many shells
as you like using `bin/console`. If your test includes a web server (try `lein
run serve` on the control node, in your test directory), you can open it
locally by running using `bin/web`. This can be a handy way to browse test
results.

## Advanced

If you need to log into a DB node (e.g. to debug a test), you can `ssh n1` (or n2, n3, ...) from inside the control node, or:

```
docker exec -it jecci-n1 bash
```

During development, it's convenient to run with `--dev` option, which mounts `$JECCI_ROOT` dir as `/jecci` on Jecci control container.

Run `./up.sh --help` for more info.
