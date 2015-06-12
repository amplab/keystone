#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
pushd $FWDIR

export SPARK_HOME="/root/spark"
export KEYSTONE_MEM=150g

FEATURES_TRAIN_DIR="/imagenet-tempates-256-160/featuresTrain"
FEATURES_VAL_DIR="/imagenet-tempates-256-160/featuresTest"
LABELS_TRAIN_DIR="/imagenet-tempates-256-160/trainLabels"
LABELS_ACTUAL_DIR="/imagenet-tempates-256-160/testActual"

LOG_SUFFIX=`date +"%Y_%m_%d_%H_%M_%S"`

./bin/run-pipeline.sh \
  pipelines.solvers.BlockWeightedSolver \
  --trainFeaturesDir $FEATURES_TRAIN_DIR \
  --trainLabelsDir $LABELS_TRAIN_DIR \
  --testFeaturesDir $FEATURES_VAL_DIR \
  --testActualDir $LABELS_ACTUAL_DIR \
  --lambda 6e-5 \
  --mixtureWeight 0.25 2>&1 | tee /mnt/solver-logs-"$LOG_SUFFIX".log
