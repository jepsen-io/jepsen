#!/usr/bin/env python

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

# This script aims to be compatible with both Python 3.


"""
A script to run multiple YugaByte DB Jepsen tests in a loop and organize the results.
"""

import argparse
import json
import logging
import os
import re
import subprocess
from collections import namedtuple

import atexit
import errno
import sys
import time
from itertools import zip_longest, chain

import requests
from junit_xml import TestCase, TestSuite, to_xml_report_string

CmdResult = namedtuple('CmdResult',
                       ['output',
                        'returncode',
                        'timed_out',
                        'everything_looks_good'])

# Only for workload, doesn't include test results analysis. Customized for the "set" test.
SINGLE_TEST_RUN_TIME = 600

# The set test might time out if you let it run for 10 minutes and leave 10 more
# minutes for analysis, so cut its running time in half.
SINGLE_TEST_RUN_TIME_FOR_SET_TEST = 300

TEST_AND_ANALYSIS_TIMEOUT_SEC = 1200  # Includes test results analysis.
DEFAULT_TARBALL_URL = "https://downloads.yugabyte.com/yugabyte-1.3.1.0-linux.tar.gz"

TEST_PER_VERSION = [
    {
        "start_version": "1.3.1.0",
        "tests": [
            # YCQL snapshot isolation
            "ycql/counter",
            "ycql/set",
            "ycql/set-index",
            "ycql/bank",
            "ycql/long-fork",
            "ycql/single-key-acid",
            "ycql/multi-key-acid",
            # Disabled https://github.com/yugabyte/yugabyte-db/issues/10328
            # Related to multipage index scan https://github.com/yugabyte/yugabyte-db/issues/13502
            # "ycql/bank-inserts",

            # YSQL serializable
            "ysql/sz.counter",
            "ysql/sz.set",
            "ysql/sz.bank",
            "ysql/sz.bank-contention",
            "ysql/sz.bank-multitable",
            "ysql/sz.long-fork",
            "ysql/sz.single-key-acid",
            "ysql/sz.multi-key-acid",
            "ysql/sz.default-value",
            "ysql/sz.ol.append",
            "ysql/sz.ol.geo.append",

            # YSQL snapshot isolation
            "ysql/si.ol.append",
            "ysql/si.ol.geo.append",
            "ysql/si.bank",
            "ysql/si.bank-contention",
            "ysql/si.bank-multitable",
        ]
    },
    {
        "start_version": "2.13.1.0-b1",
        "tests": [
            # YSQL read committed
            "ysql/rc.ol.append",
            "ysql/rc.ol.geo.append",
            "ysql/rc.pl.append",
            "ysql/rc.pl.geo.append",
        ]
    },
    {
        "start_version": "2.17.2.0-b1",
        "tests": [
            "ysql/sz.pl.append",
            "ysql/sz.pl.geo.append",
            "ysql/si.pl.append",
            "ysql/si.pl.geo.append",
        ]
    }
]
NEMESES = [
    "none",
    "kill-tserver",
    "kill-master",
    "pause-tserver",
    "pause-master",
    "partition",
    # "clock-skew",
]
TESTS = list(chain(*[test["tests"] for test in TEST_PER_VERSION]))

SCRIPT_DIR = os.path.abspath(os.path.dirname(sys.argv[0]))
STORE_DIR = os.path.join(SCRIPT_DIR, "store")
LOGS_DIR = os.path.join(SCRIPT_DIR, "logs")
SORT_RESULTS_SH = os.path.join(SCRIPT_DIR, "sort-results.sh")
REGEX_MAJOR_VERSION = r"^(\d+)\.(\d+)"

child_processes = []


def get_workload_version(workload):
    for el in TEST_PER_VERSION:
        for tests in el["tests"]:
            if workload in tests:
                return el["start_version"]
    raise EnvironmentError(f"Unanable to find workload in tests: {TESTS}")


def is_version_at_least(v_least, v_actual):
    v_least_split = re.split('\.|-b', v_least)
    v_actual_split = re.split('\.|-b', v_actual)
    return next((i < j
                 for i, j in zip_longest(map(int, v_least_split), map(int, v_actual_split),
                                         fillvalue=0) if i != j), True)


def cleanup():
    deadline = time.time() + 5
    for p in child_processes:
        while p.poll() is None and time.time() < deadline:
            time.sleep(1)
        try:
            p.kill()
        except OSError as e:
            if e.errno != errno.ESRCH:
                raise e


def truncate_line(line, max_chars=500):
    if len(line) <= max_chars:
        return line
    res_candidate = line[:max_chars] + "... (skipped %d chars)" % (len(line) - max_chars)

    return line if len(line) <= len(res_candidate) else res_candidate


def get_last_lines(file_path, n_lines):
    total_num_lines = int(subprocess.check_output(['wc', '-l', file_path]).strip().split()[0])
    return (
        subprocess.check_output(['tail', '-n', str(n_lines), file_path]).decode().split("\n"),
        total_num_lines
    )


def show_last_lines(file_path, n_lines):
    if n_lines is None:
        return
    if not os.path.exists(file_path):
        logging.warning("File does not exist: %s, cannot show last %d lines",
                        file_path, n_lines)
        return
    lines, total_num_lines = get_last_lines(file_path, n_lines)
    logging.info(
        "%s of file %s:\n%s",
        "Last %d lines" % n_lines if total_num_lines > n_lines else 'Contents',
        file_path,
        "\n".join([truncate_line(line) for line in lines])
    )


def send_report_to_reportportal(
        xml_report_name,
        xml_report_content,
        reportportal_base_url,
        reportportal_project_name,
        reportportal_api_token,
        version,
        jenkins_url,
):
    full_version = version.split("-b")[0]
    major_version = ".".join(re.findall(REGEX_MAJOR_VERSION, version)[0])
    build_version = version.split("-b")[1]

    url = f"{reportportal_base_url}/api/v1/{reportportal_project_name}/launch/import"

    response = requests.post(
        url,
        files={
            'file': (f"{major_version}-{xml_report_name}", xml_report_content),
            'type': 'text/xml'},
        headers={"accept": "*/*",
                 "Authorization": f"bearer {reportportal_api_token}"}
    )

    if response.status_code == 200:
        launch_uuid = re.search('(?<=id = )[^ ]+', json.loads(response.text)["message"])[0]
        logging.info(f"Successfully posted launch {launch_uuid}")
    else:
        logging.error(
            f"Can't send data to the ReportPortal {reportportal_base_url} due to {response.text} "
            f"(code {response.status_code})")
        return False

    # Need to translate UUID to launch-specific ID
    url = f"{reportportal_base_url}/api/v1/{reportportal_project_name}/launch/uuid/{launch_uuid}"

    response = requests.get(url, headers={"accept": "*/*",
                                          "Authorization": f"bearer {reportportal_api_token}"})

    if response.status_code == 200:
        launch_id = json.loads(response.text)["id"]
        logging.info(f"Successfully found launch ID {launch_id}")
    else:
        logging.error(f"Can't find launch ID for uuid {launch_uuid}")
        logging.error(f"Code: {response.status_code} Text: {response.text}")
        return False

    url = f"{reportportal_base_url}/api/v1/{reportportal_project_name}/launch/{launch_id}/update"

    response = requests.put(url, json={"attributes": [{"key": "version", "value": full_version},
                                                      {"key": "build", "value": build_version},
                                                      {"key": "jenkins", "value": jenkins_url}]},
                            headers={"accept": "*/*", "Content-Type": "application/json",
                                     "Authorization": f"bearer {reportportal_api_token}"})

    if response.status_code == 200:
        logging.info(f"Successfully updated attributes for launch {launch_id}")
    else:
        logging.error(f"Could not update attributes for launch {launch_id}")
        logging.error(f"Code: {response.status_code} Text: {response.text}")
        return False


def run_cmd(cmd,
            timeout=None,
            exit_on_error=True,
            log_name_prefix=None,
            num_lines_to_show=None):
    logging.info("Running command: %s", cmd)
    stdout_path = None
    stderr_path = None
    keep_output_log_file = True
    if log_name_prefix is not None:
        stdout_path = os.path.join(LOGS_DIR, f'{log_name_prefix}_stdout.log')
        stderr_path = os.path.join(LOGS_DIR, f'{log_name_prefix}_stderr.log')
        logging.info("stdout log: %s", stdout_path)
        logging.info("stderr log: %s", stderr_path)

    stdout_file = None
    stderr_file = None

    popen_kwargs = dict(shell=True)
    try:
        if log_name_prefix is None:
            p = subprocess.Popen(cmd, **popen_kwargs)
        else:
            stdout_file = open(stdout_path, 'wb')
            stderr_file = open(stderr_path, 'wb')
            p = subprocess.Popen(cmd, stdout=stdout_file, stderr=stderr_file, **popen_kwargs)

        child_processes.append(p)

        deadline = time.time() + timeout if timeout else float('inf')
        while p.poll() is None and (timeout is None or time.time() < deadline):
            time.sleep(1)

        if p.poll() is None:
            timed_out = True
            p.kill()
            returncode = p.wait()
        else:
            timed_out = False
            returncode = p.returncode

        child_processes.remove(p)
        if returncode != 0:
            logging.error("Failed running command (exit code: %d): %s", returncode, cmd)
            if exit_on_error:
                sys.exit(returncode)
        everything_looks_good = False
        last_lines_of_output = []
        if stdout_path is not None and os.path.exists(stdout_path):
            last_lines_of_output, _ = get_last_lines(stdout_path, 50)
            everything_looks_good = any(
                line.startswith('Everything looks good!') for line in last_lines_of_output)
        if everything_looks_good:
            keep_output_log_file = False
        return CmdResult(
            output=None if everything_looks_good else "\n".join(
                [truncate_line(line) for line in last_lines_of_output]),
            returncode=returncode,
            timed_out=timed_out,
            everything_looks_good=everything_looks_good)

    finally:
        if stdout_file is not None:
            stdout_file.close()
            show_last_lines(stdout_path, num_lines_to_show)
            if not keep_output_log_file:
                try:
                    os.remove(stdout_path)
                except IOError as ex:
                    logging.error("Error deleting output log %s, ignoring: %s", stdout_path, ex)
        if stderr_file is not None:
            stderr_file.close()
            show_last_lines(stderr_path, num_lines_to_show)
            if not keep_output_log_file:
                try:
                    os.remove(stderr_path)
                except IOError as ex:
                    logging.error("Error deleting stderr log %s, ignoring: %s", stderr_path, ex)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--build_url',
        default="",
        help='Jenkins build URL')
    parser.add_argument(
        '--reportportal_base_url',
        default="",
        help='ReportPortal base URL')
    parser.add_argument(
        '--reportportal_project_name',
        default="",
        help='ReportPortal project name')
    parser.add_argument(
        '--reportportal_api_token',
        default="",
        help='ReportPortal API token')
    parser.add_argument(
        '--url',
        default=DEFAULT_TARBALL_URL,
        help='YugaByte DB tarball URL to use')
    parser.add_argument(
        '--max-time-sec',
        type=int,
        help='Maximum time to run for. The actual run time could be a few minutes longer than '
             'this.')
    parser.add_argument(
        '--enable-clock-skew',
        action='store_true',
        help='Enable clock skew nemesis. This will not work on LXC.')
    parser.add_argument(
        '--concurrency',
        default='4n',
        help='Concurrency to specify, e.g. 2n, 4n, or 5n, where n means the number of nodes.')
    parser.add_argument(
        '--workloads',
        default=','.join(TESTS),
        help='Comma-seperated list of workloads. Default: ' + ','.join(TESTS))
    parser.add_argument(
        '--nemeses',
        default=','.join(NEMESES),
        help='Comma-seperated list of nemeses. Default: ' + ','.join(NEMESES))
    parser.add_argument(
        '--iterations',
        type=int,
        help='Run each workload repeatedly for this many iterations.')
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(filename)s:%(lineno)d %(levelname)s] %(message)s")
    args = parse_args()

    atexit.register(cleanup)

    # Sort old results in the beginning if it did not happen at the end of the last run.
    run_cmd(SORT_RESULTS_SH)

    start_time = time.time()
    nemeses = args.nemeses
    if args.enable_clock_skew:
        nemeses += ',clock-skew'

    num_tests_run = 0
    num_timed_out_tests = 0
    total_test_time_sec = 0

    if os.path.isdir(LOGS_DIR):
        logging.info(f"Directory {LOGS_DIR} already exists", )
    else:
        logging.info(f"Creating directory {LOGS_DIR}")
        os.mkdir(LOGS_DIR)

    test_index = 0
    num_everything_looks_good = 0
    num_not_everything_looks_good = 0
    num_zero_exit_code = 0
    num_non_zero_exit_code = 0
    url = args.url

    version = None
    for match in re.finditer(r"(?<=yugabyte-)(\d+\.\d+(\.\d+){0,2}(-b\d+)?)", url, re.MULTILINE):
        version = match.group()
        break
    if version is None:
        raise AttributeError(f"Failed to parse version from URL {url}")

    not_good_tests = []
    lein_cmd = " ".join(["lein run test",
                         "--os debian",
                         f"--url {url}",
                         f"--nemesis {nemeses}",
                         f"--ssh-private-key ~/.ssh/id_rsa", # tmp workaround for jepsen 0.2.7+ versions
                         f"--concurrency {args.concurrency}"])

    if args.iterations:
        lein_cmd += " --test-count 1"
        iteration_cnt = args.iterations
    else:
        iteration_cnt = 1

    all_workloads = args.workloads.split(',')
    workloads_to_evaluate = [workload for workload in all_workloads
                             if is_version_at_least(get_workload_version(workload),
                                                    version)]
    workloads_to_skip = set(all_workloads) - set(workloads_to_evaluate)

    if not workloads_to_evaluate:
        logging.error(
            f"No workloads for evaluate have been found because of version incompatibility\n"
            f"Should be skipped: {workloads_to_skip}\n"
            f"Workloads to evaluate: {workloads_to_evaluate}")
        exit(1)

    test_cases = {}
    for test in workloads_to_evaluate:
        for iteration in range(iteration_cnt):
            total_elapsed_time_sec = time.time() - start_time
            if args.max_time_sec is not None and total_elapsed_time_sec > args.max_time_sec:
                logging.info(
                    "Elapsed time is %.1f seconds, it has exceeded the max allowed time %.1f, "
                    "stopping", total_elapsed_time_sec, args.max_time_sec)
                break

            test_index += 1
            test_description_str = f"workload {test}, nemesis {nemeses}"
            logging.info(
                "\n%s\nStarting test run #%d - %s\n%s",
                "=" * 80,
                test_index,
                test_description_str,
                "=" * 80)
            test_start_time_sec = time.time()
            if '/set' in test:
                test_run_time_limit_no_analysis_sec = SINGLE_TEST_RUN_TIME_FOR_SET_TEST
            else:
                test_run_time_limit_no_analysis_sec = SINGLE_TEST_RUN_TIME
            full_cmd = lein_cmd + \
                       " --time-limit " + str(test_run_time_limit_no_analysis_sec) + \
                       " --workload " + test
            result = run_cmd(
                full_cmd,
                timeout=TEST_AND_ANALYSIS_TIMEOUT_SEC,
                exit_on_error=False,
                log_name_prefix=f"{test.replace('/', '-')}_nemesis_{nemeses}_{test_index}",
                num_lines_to_show=30)

            test_elapsed_time_sec = time.time() - test_start_time_sec
            if result.timed_out:
                jepsen_log_file = os.path.join(STORE_DIR, 'current', 'jepsen.log')
                num_timed_out_tests += 1
                logging.info("Test timed out. Updating the log at %s", jepsen_log_file)
                if os.path.exists(jepsen_log_file):
                    msg = "Test run timed out!"
                    logging.info(msg)
                    with open(jepsen_log_file, "a") as f:
                        f.write(msg)
                else:
                    logging.error("File %s does not exist!", jepsen_log_file)

            test_name = f"{test}_{nemeses}"
            tc = TestCase(name=test_name,
                          classname=test.split('/')[0],
                          elapsed_sec=test_elapsed_time_sec,
                          url=args.build_url,
                          stderr=result.output)
            logging.info(
                "Test run #%d: elapsed_time=%.1f, returncode=%d, everything_looks_good=%s",
                test_index, test_elapsed_time_sec, result.returncode,
                result.everything_looks_good)

            if result.everything_looks_good:
                num_everything_looks_good += 1

                if test_name not in test_cases:
                    test_cases[test_name] = tc
            else:
                tc.add_error_info(test_description_str)
                num_not_everything_looks_good += 1
                not_good_tests.append(test_description_str)

                if result.timed_out:
                    message = "Timed out"

                    tc.add_error_info(message)
                elif not result.everything_looks_good:
                    message = "Failure on result validation"

                    tc.add_error_info(message)
                else:
                    message = f"Process exited with error code {result.returncode}"

                    tc.add_failure_info(message, failure_type="exit code")

                # always add latest failed run for the results
                test_cases[test_name] = tc

            if result.returncode == 0:
                num_zero_exit_code += 1
            else:
                num_non_zero_exit_code += 1

            run_cmd(f"{SORT_RESULTS_SH} {nemeses}")

            logging.info(
                "\n%s\nFinished test run #%d (%s)\n%s",
                "=" * 80, test_index, test_description_str, "=" * 80)

            num_tests_run += 1
            total_test_time_sec += test_elapsed_time_sec

            total_elapsed_time_sec = time.time() - start_time
            logging.info("Finished running %d tests.", num_tests_run)
            logging.info("    %d okay, %d problems (%d timed-out)",
                         num_everything_looks_good, num_not_everything_looks_good,
                         num_timed_out_tests)
            logging.info("    %d tests (out of %d total) returned non-zero exit code",
                         num_non_zero_exit_code, num_tests_run)
            logging.info("Elapsed time: %.1f sec, test time: %.1f sec, avg test time: %.1f sec",
                         total_elapsed_time_sec, total_test_time_sec,
                         total_test_time_sec / num_tests_run)
            if not_good_tests:
                logging.info("Tests where something does not look good:\n    %s",
                             "\n    ".join(not_good_tests))
        else:
            # Next workload
            continue
        # Inner loop broken, skip remaining workloads
        break

    logging.warning(f"Skipped workloads because of version incompatibility {workloads_to_skip}")

    logging.info("Sending JUnit XML report")
    ts = TestSuite(f"Jepsen {nemeses.replace(',', '-')} {version}", test_cases.values())
    if args.reportportal_base_url and args.reportportal_project_name and args.reportportal_api_token:
        send_report_to_reportportal(f"jepsen-junit-{nemeses.replace(',', '-')}.xml",
                                    to_xml_report_string([ts]),
                                    args.reportportal_base_url,
                                    args.reportportal_project_name,
                                    args.reportportal_api_token,
                                    version,
                                    args.build_url)
    else:
        logging.warning("Skipped ReportPortal reporting due to missing args")

    logging.info("Storing JUnit XML reports locally")
    with open(f"jepsen-junit-{nemeses.replace(',', '-')}.xml", "w") as xml_report:
        xml_report.write(to_xml_report_string([ts]))

    if not_good_tests:
        exit(1)
    else:
        exit(0)


if __name__ == '__main__':
    main()
