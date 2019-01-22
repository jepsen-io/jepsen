#!/bin/bash

set -euo pipefail

readonly SAVED_DIR=$(pwd)

readonly SCRIPT_DIR="${0%/*}"
readonly STORE_DIR="$SCRIPT_DIR/store"
readonly SORTED_DIR="$SCRIPT_DIR/results-sorted"

mkdir -p $STORE_DIR

trap 'cd $SAVED_DIR' EXIT SIGHUP SIGINT SIGTERM

cd "${0%/*}"

find $STORE_DIR -name "jepsen.log" -printf "%T+\t%p\n" | sort | cut -f2 |
  while IFS= read -r log_path; do
    rel_log_path=${log_path#$STORE_DIR/}
    rel_dir_path=${rel_log_path%/jepsen.log}
    if grep -q ':valid? false' "$log_path"; then
      category="invalid"
    elif grep -q ':valid? :unknown' "$log_path"; then
      category="valid-unknown"
    elif grep -q 'Test run timed out!' "$log_path"; then
      category="timed-out"
    elif grep -q 'Everything looks good!' "$log_path"; then
      category="ok"
    elif grep -q 'jepsen.os.OS.install_build_essential_BANG_' "$log_path"; then
      category="no-such-method"
    elif grep -q 'Caused by: java.lang.AssertionError: Assert failed: invocation value' "$log_path"; then
      category="assert-failed-invocation-value"
    elif grep -q 'set[!]: [*]current-length[*] from non-binding thread' "$log_path"; then
      category="cant-set-current-length"
    elif [[ ! -e "$STORE_DIR/$rel_dir_path/history.edn" ]]; then
      category="no-history"
    else
      category="unknown"
    fi
    dest_dir="$SORTED_DIR/$category/$rel_dir_path"
    mkdir -p "$(dirname "$dest_dir")"
    mv "$STORE_DIR/$rel_dir_path" "$dest_dir"
    rm -f "$SORTED_DIR/latest"
    ln -sf "../$dest_dir" "$SORTED_DIR/latest"
  done
