#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
pushd $FWDIR

IMAGENET_TRAIN_DIR="/imagenet-train-all-scaled-tar"
IMAGENET_VAL_DIR="/imagenet-validation-all-scaled-tar"
IMAGENET_LABELS="/root/keystone/src/main/resources/imagenet-labels"

./bin/run-pipeline.sh \
  pipelines.images.imagenet.ImageNetSiftLcsFV \
  --trainLocation $IMAGENET_TRAIN_DIR \
  --testLocation $IMAGENET_VAL_DIR \
  --labelPath $IMAGENET_LABELS \ 
  --numPcaSamples 10000000 \
  --numGmmSamples 10000000

popd
