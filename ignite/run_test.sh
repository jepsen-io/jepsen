#!/usr/bin/env bash
lein run test \
  --test bank \
  --time-limit 10 \
  --concurrency 5 \
  --nodes-file nodes \
  --username root \
  --password root \
  --cache-mode PARTITIONED \
  --cache-atomicity-mode TRANSACTIONAL \
  --cache-write-sync-mode FULL_ASYNC \
  --read-from-backup false \
  --transaction-concurrency OPTIMISTIC \
  --transaction-isolation SERIALIZABLE \
  --backups 3 \
  --os noop \
  --nemesis noop \
  --version 2.8.0 \
  --pds false
