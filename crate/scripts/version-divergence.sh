#! /bin/sh

lein run test --test version-divergence --concurrency 2n --tarball $1
