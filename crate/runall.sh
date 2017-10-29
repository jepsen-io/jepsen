#! /bin/sh

EXIT_STATUS=0
for test in "dirty-read" "lost-updates" "version-divergence"
do
    lein run test --test ${test} --concurrency 2n --tarball $1
    EXIT_STATUS=$(expr ${EXIT_STATUS} + $?)
    sleep 15
done

exit ${EXIT_STATUS}
