#! /usr/bin/env bash
sqllog=/home/ubuntu/logs/sql.log
refts=$1
shift
cd /home/ubuntu
test -e $sqllog || touch $sqllog
sudo chmod a+rw $sqllog
echo -n `date -Ins` :: $refts :: >>$sqllog
for stm in "$@"; do 
   echo -n " $stm;\\" >>$sqllog
done
echo >>$sqllog
/home/ubuntu/cockroach sql -e "$@" >sql.res 2>sql.err
code=$?
cat sql.res
echo "`date -Ins` :: $refts :: RESULT: $(tr '\n' '\\' <sql.res)" >>$sqllog
echo "`date -Ins` :: $refts :: EXIT: $code ERR $(tr '\n' '\\' <sql.err)" >>$sqllog
if test $code = 0; then
   exit 0
else
   cat sql.err | grep -v insecure
   exit $code
fi
