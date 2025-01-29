#!/usr/bin/env bash

node=$1
ts=$(date -Iseconds)
dir="stats/${node}/${ts}"
echo $dir
mkdir -p "${dir}"
cd "${dir}"
ssh "${node}" free -m > free
ssh "${node}" sudo netstat -ant | grep 9100 > conns
# ssh "${node}" sudo pmap -x "$(ps aux | grep yb-tserver | grep -v grep | awk '{ print $2 }')" > pmap
ssh "${node}" curl -s "http://localhost:9000/rpcz" > rpcz
ssh "${node}" curl -s "http://localhost:9000/metrics" > metrics
ssh "${node}" curl -s "http://localhost:9000/mem-trackers" > mem-trackers
ssh "${node}" curl -s "http://localhost:9000/threadz?group=all" > threadz
ssh "${node}" curl -s "http://localhost:9000/pprof/contention" > contention
cd "../../"
