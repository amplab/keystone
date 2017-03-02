#!/bin/bash
set -e

#Set environment variables
: ${KEYSTONE_MEM:=4g}
export KEYSTONE_MEM

: ${NUM_FFTS:=4}
: ${BLOCK_SIZE:=2048}

KEYSTONE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../..
: ${EXAMPLE_DATA_DIR:=$KEYSTONE_DIR/example_data}

if [ ! -d $EXAMPLE_DATA_DIR ]; then
    mkdir $EXAMPLE_DATA_DIR
fi

# Get the data from S3
if [ ! -f $EXAMPLE_DATA_DIR/train-mnist-dense-with-labels.data ]; then
    wget -O $EXAMPLE_DATA_DIR/train-mnist-dense-with-labels.data  \
        http://mnist-data.s3.amazonaws.com/train-mnist-dense-with-labels.data
fi

if [ ! -f $EXAMPLE_DATA_DIR/test-mnist-dense-with-labels.data ]; then
    wget -O $EXAMPLE_DATA_DIR/test-mnist-dense-with-labels.data \
        http://mnist-data.s3.amazonaws.com/test-mnist-dense-with-labels.data
fi

$KEYSTONE_DIR/bin/run-pipeline.sh \
  keystoneml.pipelines.images.mnist.MnistRandomFFT \
  --trainLocation $EXAMPLE_DATA_DIR/train-mnist-dense-with-labels.data \
  --testLocation $EXAMPLE_DATA_DIR/test-mnist-dense-with-labels.data \
  --numFFTs $NUM_FFTS \
  --blockSize $BLOCK_SIZE
