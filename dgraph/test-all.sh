#!/bin/bash
TIME_LIMIT=3600 # 1 hour per workload. There are 8 workloads total.
TEST_COUNT=1
for i in $(seq 1 $TEST_COUNT); do
    for workload in bank delete long-fork linearizable-register uid-linearizable-register upsert set sequential; do
        if echo $workload | grep -q -E 'delete|linearizable-register$'; then
            concurrency=30
        else
            concurrency=1n
        fi
        lein run test \
             --local-binary /gobin/dgraph \
             --force-download \
             --upsert-schema \
             --rebalance-interval 10h \
             --nemesis-interval 60 \
             --nemesis move-tablet,partition-ring,kill-alpha,kill-zero,skew-clock \
             --skew big \
             --workload $workload \
             --time-limit $TIME_LIMIT \
             --concurrency $concurrency
    done
done
