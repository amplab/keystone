#!/bin/bash
set -e

#Set environment variables
: ${KEYSTONE_MEM:=4g}
export KEYSTONE_MEM

KEYSTONE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../..
: ${EXAMPLE_DATA_DIR:=$KEYSTONE_DIR/example_data}

if [ ! -d $EXAMPLE_DATA_DIR ]; then
    mkdir $EXAMPLE_DATA_DIR
fi

#Download data if necessary.
if [[ ! ( -f $EXAMPLE_DATA_DIR/cifar_train.bin && -f $EXAMPLE_DATA_DIR/cifar_test.bin ) ]]; then
    #Get the data
    wget -O $TMPDIR/cifar-10-binary.tar.gz  http://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz 

    #Decompress it
    tar zxvf $TMPDIR/cifar-10-binary.tar.gz -C $TMPDIR
    cat $TMPDIR/cifar-10-batches-bin/data_batch*.bin > $EXAMPLE_DATA_DIR/cifar_train.bin
    mv $TMPDIR/cifar-10-batches-bin/test_batch.bin $EXAMPLE_DATA_DIR/cifar_test.bin
       
    #Clean up. 
    rm -rf $TMPDIR/cifar-10-batches-bin
    rm -rf $TMPDIR/cifar-10-binary.tar.gz
fi

#Run the pipeline
$KEYSTONE_DIR/bin/run-pipeline.sh \
  keystoneml.pipelines.images.cifar.RandomPatchCifar \
  --trainLocation $EXAMPLE_DATA_DIR/cifar_train.bin \
  --testLocation $EXAMPLE_DATA_DIR/cifar_test.bin \
  --numFilters 10000 \
  --lambda 3000 \
  --whiteningEpsilon 1e-5
