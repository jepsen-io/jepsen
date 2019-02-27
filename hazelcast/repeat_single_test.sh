#!/usr/bin/env bash

if [ $# != 3 ]; then
	echo "how to use: ./repeat_single_test.sh test_name repeat test_duration"
	exit 1
fi

test_name=$1
repeat=$2
test_duration=$3
round="1"

while [ ${round} -le ${repeat} ]; do

    echo "round: $round"

    echo "running $test_name test"

    lein run test --workload ${test_name} --time-limit ${test_duration}

    if [ $? != '0' ]; then
        echo "$test_name test failed"
        exit 1
    fi

    round=`expr $round \+ 1`

done
