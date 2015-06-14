#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
pushd $FWDIR

export SPARK_HOME="/root/spark"
export KEYSTONE_MEM=150g

FEATURES_TRAIN_DIR="/imagenet-tempates-256-160/featuresTrain"
FEATURES_VAL_DIR="/imagenet-tempates-256-160/featuresTest"
LABELS_TRAIN_DIR="/imagenet-tempates-256-160/trainLabels"
LABELS_ACTUAL_DIR="/imagenet-tempates-256-160/testActual"

for i in 1e-7 #1e-6 #1e-5 1e-6 1e-4
do
    for j in 1.5e-1 #5e-2 3e-1 2.5e-1 #1e-1 5e-2 3e-1 2.5e-1
    do
       LOG_SUFFIX=`date +"%Y_%m_%d_%H_%M_%S"`

       ./bin/run-pipeline.sh \
           pipelines.solvers.BlockWeightedSolver \
           --trainFeaturesDir $FEATURES_TRAIN_DIR \
           --trainLabelsDir $LABELS_TRAIN_DIR \
           --testFeaturesDir $FEATURES_VAL_DIR \
           --testActualDir $LABELS_ACTUAL_DIR \
           --lambda $i \
           --mixtureWeight $j 2>&1 | tee /mnt/solver-logs-"$i"-"$j"-"$LOG_SUFFIX".log

       echo "Restarting spark"
       /root/spark/sbin/stop-all.sh
       sleep 20
       /root/spark/sbin/start-all.sh
       sleep 20

    done
done

popd

# To use tuples instead of a grid use something like the following code
#
# OLDIFS=$IFS; IFS=',';
# for i in 1.3e-7,0.15 3.2e-7,0.1 3e-8,0.1 3.2e-7,0.1 1.3e-7,0.6 1e-8,0.75 2.4e-7,0.75 6e-8,0.125
# do
#     set $i
#     lambda=$1
#     wt=$2
#
#     # Run pipeline here with $lambda and $wt
#
# done
# IFS=$OLDIFS
