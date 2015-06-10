#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
pushd $FWDIR

IMAGENET_TRAIN_DIR="/imagenet-train-all-scaled-tar"
IMAGENET_VAL_DIR="/imagenet-validation-all-scaled-tar"
IMAGENET_LABELS="/root/keystone/src/main/resources/imagenet-labels"

./bin/run-pipeline.sh \
  pipelines.images.imagenet.ImageNetSiftLcsTemplateInteractions \
  --trainLocation $IMAGENET_TRAIN_DIR \
  --testLocation $IMAGENET_VAL_DIR \
  --labelPath $IMAGENET_LABELS \
  --numZcaSamples 5000000 \ 
  --lambda 6e-5 \
  --mixtureWeight 0.25 \
  --numKMeans 256 \
  --numGaussianRandomFeatures 160

popd
