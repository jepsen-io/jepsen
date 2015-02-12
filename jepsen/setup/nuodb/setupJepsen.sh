#!/bin/bash
#
#Sample bash script to setup a nuodb database running a TE and SM on each of 5 hosts
#
BROKER="n1"
TIMEOUT=30
ARCHIVE_DIR="/tmp/jepsen"
DOMAIN_PW="bird"
NUODBMANAGER_JAR="/opt/nuodb/jar/nuodbmanager.jar"
declare -a HOSTS=("n1" "n2" "n3" "n4" "n5")

for HOST in ${HOSTS[@]}
do
    java -jar $NUODBMANAGER_JAR --broker $BROKER --password $DOMAIN_PW --command "start process sm host $HOST database jepsen archive $ARCHIVE_DIR initialize yes options '--ping-timeout $TIMEOUT --commit remote:3 --verbose error,warn,flush'"
done

for HOST in ${HOSTS[@]}
do
    java -jar $NUODBMANAGER_JAR --broker $BROKER --password $DOMAIN_PW --command "start process te host $HOST database jepsen options '--ping-timeout $TIMEOUT --commit remote:3 --dba-user jepsen --dba-password jepsen --verbose error,warn,flush'"
done

