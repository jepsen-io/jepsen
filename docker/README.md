Dockerized Jepsen
=================

This docker image attempts to simplify the setup required by Jepsen.
It is intended to be used by a CI tool or anyone with docker who wants to try jepsen themselves.

It contains all the jepsen dependencies and code. It uses [docker-in-docker](https://github.com/jpetazzo/dind) to spin up the five
containers used by Jepsen.  

To start run

````
    docker run --privileged -t -i tjake/jepsen
````







