# Uncomment this if you uncomment the matching line in setup.sh.
# tc qdisc change dev eth0 root netem delay 0ms
killall rethinkdb
f=0
# Wait for all rethinkdb instances to go away.
while [[ "`ps auxww | grep rethinkdb | grep -v grep`" ]] && [[ "$f" -lt 10 ]]; do
  sleep 1
  f=$((f+1))
done
# If it takes more than 10 seconds, murder them.
if [[ "$f" -ge 10 ]]; then
  killall -9 rethinkdb
fi
