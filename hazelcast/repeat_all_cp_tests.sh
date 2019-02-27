#!/usr/bin/env bash

if [ $# != 2 ]; then
	echo "how to use: ./repeat_all_cp_tests.sh repeat test_duration"
	exit 1
fi

repeat=$1
test_duration=$2

run_single_test () {
    test_name=$1
    echo "running $test_name test"

    lein run test --workload ${test_name} --time-limit ${test_duration}

    if [ $? != '0' ]; then
        echo "$test_name test failed"
        exit 1
    fi
}

round="1"

while [ ${round} -le ${repeat} ]; do

    echo "round: $round"

    run_single_test "non-reentrant-cp-lock"
    run_single_test "reentrant-cp-lock"
    run_single_test "non-reentrant-fenced-lock"
    run_single_test "reentrant-fenced-lock"
    run_single_test "cp-semaphore"
    run_single_test "cp-id-gen-long"
    run_single_test "cp-cas-long"
    run_single_test "cp-cas-reference"

    round=`expr $round \+ 1`

done
