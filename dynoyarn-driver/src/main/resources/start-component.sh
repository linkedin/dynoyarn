#!/usr/bin/env bash
#
# Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
#
printenv
set -x
# Read environment variables from set up.
component=$COMPONENT_NAME
hdfsStoragePath=$HDFS_STORAGE_PATH
rmHost=$RM_HOST
hadoopBinary=$HADOOP_BIN_ZIP_NAME
hadoopWorkload=$WORKLOAD_ZIP_NAME
nmCount=$NM_COUNT

containerID=${CONTAINER_ID##*_}
echo "Starting ${component} with ID ${containerID}"
echo "Heap size is $JAVA_HEAP_MAX"

echo "PWD is: `pwd`"

confDir=`pwd`/hadoop.zip/etc/hadoop
umask 022
baseDir="`pwd`/dyno-node"

logDir=$LOG_DIR

pidDir="$baseDir/pid"
baseHttpPort=$DYARN_RM_HTTP_PORT
baseSchedulerPort=$DYARN_RM_SCHEDULER_PORT
baseServiceRpcPort=$DYARN_RM_PORT
baseAdminPort=$DYARN_RM_ADMIN_PORT
baseTrackerPort=$DYARN_RM_TRACKER_PORT

rm -rf "$baseDir"
mkdir -p "$pidDir"
chmod 755 "$baseDir"
chmod 700 "$pidDir"
mkdir -p "$logDir"

# Check if files in tarball are nested in a root directory, and adjust hadoopHome accordingly
if [[ $(ls $hadoopBinary/ | wc -l) = "1" ]]; then
  binarySubdir=`ls $hadoopBinary/`
  hadoopBinary=$hadoopBinary/$binarySubdir
fi
hadoopHome="`pwd`/$hadoopBinary"
# Save real environment for later
hadoopConfOriginal=${HADOOP_CONF_DIR:-$confDir}
hadoopHomeOriginal=${HADOOP_HOME:-$hadoopHome}
echo "Saving original HADOOP_HOME as: $hadoopHomeOriginal"
echo "Saving original HADOOP_CONF_DIR as: $hadoopConfOriginal"
# Use to operate an hdfs command under the system Hadoop
# for now just used to upload the RM info file
function hdfs_original {
  HADOOP_HOME=${hadoopHomeOriginal} HADOOP_CONF_DIR=${hadoopConfOriginal} \
  HADOOP_HDFS_HOME=${hadoopHomeOriginal} HADOOP_COMMON_HOME=${hadoopHomeOriginal} \
  ${hadoopHomeOriginal}/bin/hdfs "$@"
}

extraClasspathDir="`pwd`/additionalClasspath/"
mkdir -p ${extraClasspathDir}
cp *.jar $extraClasspathDir

# This is where libleveldbjni is written (per-NM). Write it to a larger partition
# instead of /tmp
#tmpdir="/grid/a/tmp/hadoop-`whoami`"
tmpdir="$baseDir/tmp/hadoop-`whoami`"
mkdir $tmpdir

# Change environment variables for the Hadoop process
export HADOOP_HOME="$hadoopHome"
export PATH="$HADOOP_HOME/bin:$PATH"
export HADOOP_HDFS_HOME="$hadoopHome"
export HADOOP_COMMON_HOME="$hadoopHome"
export HADOOP_YARN_HOME="$hadoopHome"
export LIBHDFS_OPTS="-Djava.library.path=$hadoopHome/lib/native"
export HADOOP_MAPRED_HOME="$hadoopHome"
export HADOOP_CONF_DIR=${confDir}
export YARN_CONF_DIR=${confDir}
export HADOOP_LOG_DIR=${logDir}
export YARN_LOG_DIR=${logDir}
export HADOOP_PID_DIR=${pidDir}
export HADOOP_CLASSPATH="$extraClasspathDir/*"
export YARN_OPTS="-Djava.io.tmpdir=$tmpdir"
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS $JAVA_HEAP_MAX -XX:ParallelGCThreads=2 -XX:CICompilerCount=2"

export HADOOP_PREFIX="$hadoopHome"
unset YARN_CONF_DIR
unset YARN_LOGFILE
unset YARN_NICENESS
export YARN_PID_DIR=$HADOOP_PID_DIR
unset YARN_ROOT_LOGGER
unset YARN_IDENT_STRING
unset YARN_USER_CLASSPATH

echo -e "\n\n"

echo "Environment variables are set as:"
echo "(note that this doesn't include changes made by hadoop-env.sh)"
printenv
echo -e "\n\n"

# Starting from base_port, add 4*containerID (each NM will reserve 4 ports),
# then keep searching upwards for a free port
# find_available_port base_port
find_available_port() {
  basePort="$1"
  currPort=$((basePort+4*(10#$containerID)))
  while [[ $(netstat -nl | grep ":${currPort}[[:space:]]") ]]; do
    currPort=$((currPort+4))
  done
  echo "$currPort"
}

nmLocalizerPort=`find_available_port "10000"`
nmWebAppPort=`find_available_port "10001"`
nmWebAppHttpPort=`find_available_port "10002"`
nmPort=`find_available_port "10003"`

# Re-link capacity-scheduler.xml and container-executor.cfg based on LinkedIn 2.7 setup
if [ "$component" = "RESOURCE_MANAGER" ]; then
  mkdir ${baseDir}/dcs
  cp `readlink -f dynoyarn-capacity-scheduler.xml` ${baseDir}/dcs/capacity-scheduler.xml
  rm ${confDir}/capacity-scheduler.xml
  ln -s ${baseDir}/dcs/capacity-scheduler.xml ${confDir}/capacity-scheduler.xml
else
  rm ${hadoopHome}/etc/hadoop/container-executor.cfg
  ln -s `readlink -f dynoyarn-container-executor.cfg` ${hadoopHome}/etc/hadoop/container-executor.cfg
fi

# Add dynoyarn-specific variable substitutes so they can be used in user-provided dynoyarn-site.xml
# User-provided overrides are added as the arguments to task_command, so we add them here.
read -r -d '' driverConfigs <<EOF
  -D baseDir=${baseDir}
  -D rmHost=${rmHost}
  -D baseSchedulerPort=${baseSchedulerPort}
  -D baseServiceRpcPort=${baseServiceRpcPort}
  -D baseAdminPort=${baseAdminPort}
  -D baseTrackerPort=${baseTrackerPort}
  -D baseHttpPort=${baseHttpPort}
  -D nmPort=${nmPort}
  -D nmLocalizerPort=${nmLocalizerPort}
  -D containerID=${containerID}
  $@
EOF

if [ "$component" = "NODE_MANAGER" ]; then
  HADOOP_LOG_DIR=$logDir HADOOP_CLIENT_OPTS=$HADOOP_CLIENT_OPTS $hadoopHome/bin/hadoop com.linkedin.dynoyarn.SimulatedNodeManagers $driverConfigs $nmCount
  echo nodemanager
elif [ "$component" = "RESOURCE_MANAGER" ]; then
  HADOOP_PID_DIR=${pidDir} $hadoopHome/sbin/yarn-daemon.sh start resourcemanager $driverConfigs
  echo resourcemanager
fi

componentPIDFile="$pidDir/yarn-`whoami`-$component.pid"
while [ ! -f "$componentPIDFile" ]; do sleep 1; done
componentPID=`cat "$componentPIDFile"`

echo "Started $component at pid $componentPID"

function cleanup {
  echo "Cleaning up $component at pid $componentPID"
  kill -9 "$componentPID"

  if [ "$metricsTailPID" != "" ]; then
    echo "Stopping metrics streaming at pid $metricsTailPID"
    kill "$metricsTailPID"
  fi

  echo "Deleting any remaining files"
  rm -rf "$baseDir"
}

trap cleanup EXIT

echo "Waiting for parent process (PID: $PPID) OR $component process to exit"

set +x
while kill -0 ${componentPID} 2>/dev/null && kill -0 $PPID 2>/dev/null; do
  sleep 1
done
set -x

if kill -0 $PPID 2>/dev/null; then
  echo "$component process exited; continuing to finish"
  exit 1
else
  echo "Parent process exited; continuing to finish"
  exit 0
fi
