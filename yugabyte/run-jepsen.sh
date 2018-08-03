#!/usr/bin/env bash

set -euo pipefail

readonly TIMEOUT=600
readonly SAVED_DIR=$(pwd)
readonly NODES_FILE="$HOME/code/jepsen/nodes"
readonly TESTS=(
  single-row-inserts
  single-key-acid
  multi-key-acid
)
readonly NEMESES=(
  none
  start-stop-tserver
  start-kill-tserver
  start-stop-master
  start-kill-master
  start-stop-node
  start-kill-node
  partition-random-halves
  partition-random-node
  partition-majorities-ring
  small-skew
  medium-skew
  large-skew
  xlarge-skew
)
readonly SCRIPT_DIR="${0%/*}"
readonly STORE_DIR="$SCRIPT_DIR/store"
readonly COMPLETED_FLAG="completed.flag"

on_exit() {
  set +e
  echo "Exiting $0 ..."
  [ -n "$TEST_PGID" ] && kill -9 -$TEST_PGID
  cd $SAVED_DIR
}

trap "on_exit" EXIT SIGHUP SIGINT SIGTERM

cd "${0%/*}"
$SCRIPT_DIR/sort-results.sh
while true; do
  for test in "${TESTS[@]}"; do
    for nemesis in "${NEMESES[@]}"; do
      rm -f $COMPLETED_FLAG
      # Running test in new process group with setsid in order to easily kill on timeout with all child processes.
      setsid sh -c "lein run test --nodes-file $NODES_FILE --test $test --nemesis $nemesis; touch $COMPLETED_FLAG" &
      TEST_PID=$!
      TEST_PGID=$(ps -ho pgid $TEST_PID)
      if [[ "$TEST_PID" != "$TEST_PGID" ]]; then
        echo "Internal error: $TEST_PID != $TEST_PGID, expected to run test in new process group."
      fi
      (sleep $TIMEOUT; kill -- -$TEST_PGID; sleep 5; kill -9 -$TEST_PGID) &
      KILLER_PID=$!
      set +e
      wait $TEST_PID
      kill -9 $KILLER_PID
      set -e

      if [[ ! -e $COMPLETED_FLAG ]]; then
        log_path=$(find $STORE_DIR -name "jepsen.log" -printf "%T+\t%p\n" | sort | cut -f2 | tail -n1)
        msg="Test run timed out!"
        echo "$msg: $log_path"
        echo
        echo "$msg" >>"$log_path"
      else
        rm -f $COMPLETED_FLAG
      fi
      $SCRIPT_DIR/sort-results.sh
      # Small delay to be able to catch CTRL+C out of clojure.
      sleep 0.2
    done
  done
done
