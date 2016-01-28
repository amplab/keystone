#!/bin/bash

# Figure out where we are.
FWDIR="$(cd `dirname $0`; pwd)"

CLASS=$1
shift

# Set OMP_NUM_THREADS on workers and driver to something appropriate.
# This is due to OpenBLAS not handling large numbers of cores very well.
# See: https://github.com/amplab/keystone/issues/198 for more information. 

if [[ -z "$OMP_NUM_THREADS" ]]; then
  # Determine number of cores. We assume that hyperthreading is enabled and thus divide cores by two.
  unamestr=`uname`
  if [[ $unamestr == "Darwin" ]]; then
    CORES=$((`sysctl -n hw.ncpu`/2))
  elif [[ $unamestr == "Linux" ]]; then
    CORES=$((`cat /proc/cpuinfo | grep processor | wc -l`/2))
  else # Windows,BSD? Do the safest thing.
    CORES=1
  fi 
 
  # Set OMP_NUM_THREADS to MIN(32,CORES) to avoid stack smashing issues.
  export OMP_NUM_THREADS=$(($CORES>32?32:$CORES))
else
  if [[ $OMP_NUM_THREADS -gt 32 ]]; then
    echo 'Warning: setting OMP_NUM_THREADS > 32 may cause instability.'
  fi
fi

EXECUTOR_OMP_NUM_THREADS=${EXECUTOR_OMP_NUM_THREADS:-1}

if [[ -z "$SPARK_HOME" ]]; then
  echo "SPARK_HOME is not set, running pipeline locally"
  $FWDIR/run-main.sh $CLASS "$@"
else
  # TODO: Figure out a way to pass in either a conf file / flags to spark-submit
  KEYSTONE_MEM=${KEYSTONE_MEM:-1g}
  export KEYSTONE_MEM

  # Set some commonly used config flags on the cluster
  $SPARK_HOME/bin/spark-submit \
    --deploy-mode client \
    --class $CLASS \
    --driver-class-path $FWDIR/../target/scala-2.10/keystoneml-assembly-0.3.0-SNAPSHOT.jar \
    --driver-library-path $FWDIR/../lib \
    --conf spark.executor.extraLibraryPath=$FWDIR/../lib \
    --conf spark.executor.extraClassPath=$FWDIR/../target/scala-2.10/keystoneml-assembly-0.3.0-SNAPSHOT.jar \
    --conf spark.executorEnv.OMP_NUM_THREADS=$EXECUTOR_OMP_NUM_THREADS \
    --driver-memory $KEYSTONE_MEM \
    target/scala-2.10/keystoneml-assembly-0.3.0-SNAPSHOT.jar \
    "$@"
fi
