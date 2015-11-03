# # Add a 100ms delay to help find edge cases.
# tc qdisc add dev eth0 root netem delay 100ms \
#   || tc qdisc change dev eth0 root netem delay 100ms
rm -rf /root/rethinkdb_data
base=http://download.rethinkdb.com/apt/pool
version=/precise/main/r/rethinkdb/rethinkdb_2.1.5~0precise_amd64.deb
if [[ ! -f /root/rdb.deb ]]; then
  curl $base/$version > /root/rdb.deb
  dpkg -i /root/rdb.deb
fi
# JOINLINE should be defined by whatever slurps this script.
nohup /usr/bin/rethinkdb --bind all -n `hostname` $JOINLINE \
  >/root/1.out 2>/root/2.out &
sleep 1 # Wait to make sure we've actually cleared out `1.out` and `2.out`.
# Wait until we see /^Server ready/ in the log file.
tail -n+0 -F /root/1.out \
  | grep --line-buffered '^Server ready' \
  | while read line; do pkill -P $$ tail; exit 0; done
sleep 1 # You never know.
