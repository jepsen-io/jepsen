#! /bin/sh

EXIT_STATUS=0
STATUS=""
for test in "dirty-read" "lost-updates" "version-divergence"
do
    lein run test --test ${test} --concurrency 2n --tarball $1
    TEST_EXIT_STATUS=$?
    EXIT_STATUS=$(expr ${EXIT_STATUS} + ${TEST_EXIT_STATUS})
    STATUS="$STATUS \n Test ${test} exited with status ${TEST_EXIT_STATUS}"
    sleep 15
done

echo $STATUS
exit ${EXIT_STATUS}
