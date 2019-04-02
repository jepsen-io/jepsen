#! /bin/sh

# for bank and sets test, we run 300 seconds
# for register test, we run
#   1. 300 with no nemesis
#   2.  60 for start-stop-2 & parts & majority-ring nemesis
#   3.  15 for start-kill-2 nemesis
#   the number varies because it will run out of memory when checking linearizability

get_time() {
    if [ $1 = "register" ]
    then
        if [ $2 = "start-kill-2" ]
        then
            return 15
        elif [ $2 = "none" ]
        then
            return 300
        else
            return 30
        fi
    else
        return 300
    fi
}

for test in "bank" "bank-multitable" "sets" "register" "sequential"
do
    for nemesis in "none" "parts" "majority-ring" "start-stop-2" "start-kill-2"
        do
            get_time $test $nemesis
            t=$?
	        lein run test --test ${test} --nemesis ${nemesis} --time-limit ${t} --concurrency 30 --tarball $1
            if [ $? -ne 0 ]
            then
                echo ${test} ${nemesis}
                exit 1
            fi
	    sleep 15
    done
done
