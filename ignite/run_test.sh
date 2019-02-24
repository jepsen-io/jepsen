#!/bin/bash

lein run test --time-limit 10 --concurrency 6 --nodes-file nodes --username root --password root
