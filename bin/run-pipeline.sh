#!/bin/bash

# Figure out where we are.
FWDIR="$(cd `dirname $0`; pwd)"

CLASS=$1
shift

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
    --driver-class-path $FWDIR/../target/scala-2.10/keystoneml-assembly-0.2.jar \
    --driver-library-path $FWDIR/../lib \
    --conf spark.executor.extraLibraryPath=$FWDIR/../lib \
    --conf spark.executor.extraClassPath=$FWDIR/../target/scala-2.10/keystoneml-assembly-0.2.jar \
    --driver-memory $KEYSTONE_MEM \
    target/scala-2.10/keystoneml-assembly-0.2.jar \
    "$@"
fi
