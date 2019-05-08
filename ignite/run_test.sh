#!/usr/bin/env bash
lein run test \
  --test bank \
  --time-limit 10 \
  --concurrency 3 \
  --nodes-file nodes \
  --username root \
  --password root \
  --cache-mode PARTITIONED \
  --cache-atomicity-mode TRANSACTIONAL \
  --cache-write-sync-mode FULL_ASYNC \
  --read-from-backup YES \
  --transaction-concurrency OPTIMISTIC \
  --transaction-isolation SERIALIZABLE \
  --backups 3 \
  --os noop \
  --nemesis noop \
  --version 2.7.0 \
  --pds true
