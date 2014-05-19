#!/bin/bash


# TODO real function
function update_etc_hosts() {
    etchosts=$1
    num=0
    for i in n1 n2 n3 n4 n5; do
        cat >> $etchosts <<EOF
192.168.122.$((11 + $num)) $i $i.local
EOF
        num=$(($num + 1))
    done
}

# Wait for some machine to be up and running
# Up and running means we can ping the machine...
function wait_host_is_up() {
    echo -n "waiting for $1 to be up"
    while ! ping -c 1 $1 > /dev/null 2>&1 ;do
        echo -n .
        sleep 1
    done
    echo 
}

