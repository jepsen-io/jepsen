#! /bin/sh

lein run test --test lost-updates --concurrency 2n --tarball $1
