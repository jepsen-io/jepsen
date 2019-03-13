#!/bin/bash

# Runs Jepsen workloads for 1 hour each. Each workload triggers all the nemeses
# with --nemesis-interval set to 60 seconds.
#
# To run, use:
#
#     ./test-all.sh
#
# To run with Jaeger tracing with Jaeger host "jaeger"
#
#    JAEGER=true JAEGER_HOST=jaeger ./test-all.sh
#

TIME_LIMIT="${TIME_LIMIT:-3600}" # 1 hour per workload. There are 8 workloads total.
TEST_COUNT="${TEST_COUNT:-1}"
WORKLOADS=(
    bank
    delete
    long-fork
    linearizable-register
    # uid-linearizable-register
    upsert
    set
    sequential
)

# Optional Jaeger setup
JAEGER="${JAEGER:-false}" # set to true to disable
JAEGER_HOST="${JAEGER_HOST:-jaeger}"
JAEGER_OPTS=""
if [ "$JAEGER" = "true" ]; then
    JAEGER_OPTS=(
        --dgraph-jaeger-collector http://$JAEGER_HOST:14268
        --tracing http://$JAEGER_HOST:14268/api/traces
    )
fi


for _ in $(seq 1 "$TEST_COUNT"); do
    for workload in "${WORKLOADS[@]}"; do
        if echo "$workload" | grep -q -E 'delete|linearizable-register$'; then
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
             --workload "$workload" \
             --time-limit "$TIME_LIMIT" \
             --concurrency "$concurrency" \
             "${JAEGER_OPTS[@]}"
    done
done
