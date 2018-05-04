#!/bin/sh

CRATE_HEAP_SIZE=512m

# check in case a user was using this mechanism
if [ "x$CRATE_CLASSPATH" != "x" ]; then
    cat >&2 << EOF
Error: Don't modify the classpath with CRATE_CLASSPATH.
Add plugins and their dependencies into the plugins/ folder instead.
EOF
    exit 1
fi

CRATE_CLASSPATH=$CRATE_HOME/lib/*:$CRATE_HOME/lib/enterprise/*

if [ "x$CRATE_MIN_MEM" = "x" ]; then
    CRATE_MIN_MEM=256m
fi
if [ "x$CRATE_HEAP_SIZE" != "x" ]; then
    CRATE_MIN_MEM=$CRATE_HEAP_SIZE
    CRATE_MAX_MEM=$CRATE_HEAP_SIZE
fi

# min and max heap sizes should be set to the same value to avoid
# stop-the-world GC pauses during resize, and so that we can lock the
# heap in memory on startup to prevent any of it from being swapped
# out.
JAVA_OPTS="$JAVA_OPTS -Xms${CRATE_MIN_MEM}"
if [ "x$CRATE_MAX_MEM" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -Xmx${CRATE_MAX_MEM}"
fi

# new generation
if [ "x$CRATE_HEAP_NEWSIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -Xmn${CRATE_HEAP_NEWSIZE}"
fi

# max direct memory
if [ "x$CRATE_DIRECT_SIZE" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:MaxDirectMemorySize=${CRATE_DIRECT_SIZE}"
fi

# set to headless, just in case
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true"

# Force the JVM to use IPv4 stack
if [ "x$CRATE_USE_IPV4" != "x" ]; then
  JAVA_OPTS="$JAVA_OPTS -Djava.net.preferIPv4Stack=true"
fi

JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"

JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=75"
JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly"

# GC logging options
if [ "x$CRATE_USE_GC_LOGGING" != "x" ]; then
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCTimeStamps"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDateStamps"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintClassHistogram"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintTenuringDistribution"
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCApplicationStoppedTime"

  GC_LOG_DIR="$CRATE_HOME/logs";
  JAVA_OPTS="$JAVA_OPTS -Xloggc:$GC_LOG_DIR/gc.log"

  # Ensure that the directory for the log file exists: the JVM will not create it.
  if [[ ! -d "$GC_LOG_DIR" ||  ! -x "$GC_LOG_DIR" ]]; then
    cat >&2 << EOF
Error: GC log directory '$GC_LOG_DIR' does not exist or is not accessible.
EOF
    exit 1
  fi
fi

# Disables explicit GC
JAVA_OPTS="$JAVA_OPTS -XX:+DisableExplicitGC"

# Ensure UTF-8 encoding by default (e.g. filenames)
JAVA_OPTS="$JAVA_OPTS -Dfile.encoding=UTF-8"

# Use our provided JNA always versus the system one
JAVA_OPTS="$JAVA_OPTS -Djna.nosys=true"

# log4j options
JAVA_OPTS="$JAVA_OPTS -Dlog4j.shutdownHookEnabled=false -Dlog4j2.disable.jmx=true -Dlog4j.skipJansi=true"

# Disable netty recycler
JAVA_OPTS="$JAVA_OPTS -Dio.netty.noUnsafe=true -Dio.netty.noKeySetOptimization=true -Dio.netty.recycler.maxCapacityPerThread=0"
