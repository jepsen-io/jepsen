#! /bin/sh

lein run test --test dirty-read --concurrency 30 --tarball $1 --time-limit 100
