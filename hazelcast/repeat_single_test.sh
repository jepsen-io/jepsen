#!/usr/bin/env bash

if [ $# != 3 ]; then
	echo "how to use: ./repeat_single_test.sh test_name repeat test_duration"
	exit 1
fi

test_name=$1
repeat=$2
test_duration=$3

lein run test --workload ${test_name} --test-count ${repeat} --time-limit ${test_duration}
