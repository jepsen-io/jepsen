#! /usr/bin/env bash
/home/ubuntu/cockroach sql --insecure -e "$@" 2>sql.err
code=$?
if test $code = 0; then
   exit 0
else
   cat sql.err | grep -v insecure
   exit $code
fi 
