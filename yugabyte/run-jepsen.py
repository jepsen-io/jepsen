#!/usr/bin/env python2
#
# Copyright (c) YugaByte, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.
#

import atexit
import errno
import os
import subprocess
import sys
import time

from collections import namedtuple

CmdResult = namedtuple('CmdResult',
                       ['returncode',
                        'timed_out'])

SINGLE_TEST_RUN_TIME = 600 # Only for workload, doesn't include test results analysis.
TEST_TIMEOUT = 1200 # Includes test results analysis.
NODES_FILE = os.path.expanduser("~/code/jepsen/nodes")
URL = "https://downloads.yugabyte.com/yugabyte-ce-1.2.4.0-linux.tar.gz"

TESTS = [
   "single-key-acid",
   "multi-key-acid",
   "counter-inc",
   "counter-inc-dec",
   "bank",
   "set",
   "set-index",
   "long-fork"
]
NEMESES = [
    "none",
    "stop-tserver",
    "kill-tserver",
    "pause-tserver",
    "stop-master",
    "kill-master",
    "pause-master",
    "stop",
    "kill",
    "pause",
    "partition-half",
    "partition-one",
    "partition-ring",
    "partition",
    # "clock-skew",
]

SCRIPT_DIR = os.path.abspath(os.path.dirname(sys.argv[0]))
STORE_DIR = os.path.join(SCRIPT_DIR, "store")
SORT_RESULTS_SH = os.path.join(SCRIPT_DIR, "sort-results.sh")

child_processes = []

def cleanup():
    deadline = time.time() + 5
    for p in child_processes:
        while p.poll() == None and time.time() < deadline:
            time.sleep(1)
        try:
            p.kill()
        except OSError, e:
            if e.errno != errno.ESRCH:
                raise e

def run_cmd(cmd, shell=True, timeout=None, exit_on_error=True):
    print cmd
    p = subprocess.Popen(cmd, shell=True)
    child_processes.append(p)

    if timeout:
        deadline = time.time() + timeout
    while p.poll() == None and (timeout == None or time.time() < deadline):
        time.sleep(1)

    if p.poll() == None:
        timed_out = True
        p.terminate()
    else:
        timed_out = False

    child_processes.remove(p)
    if exit_on_error and p.returncode != 0:
        sys.exit(p.returncode)
    return CmdResult(returncode = p.returncode, timed_out = timed_out)

atexit.register(cleanup)

run_cmd(SORT_RESULTS_SH)

while True:
    for nemesis in NEMESES:
        for test in TESTS:
            result = run_cmd(
                "lein run test --os debian --url {URL} --workload {test} --nemesis {nemesis} --concurrency 5n "
                "--time-limit {run_time}".format(**locals()),
                timeout=TIMEOUT, run_time=SINGLE_TEST_RUN_TIME, exit_on_error=False
            )
            if result.timed_out:
                for root, dirs, files in os.walk(STORE_DIR):
                    for file in files:
                        if file == "jepsen.log":
                            msg = "Test run timed out!"
                            print "\n" + msg
                            with open(os.path.join(root, file), "a") as f:
                                f.write(msg)

            run_cmd(SORT_RESULTS_SH)

