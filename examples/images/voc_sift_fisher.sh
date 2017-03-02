#!/bin/bash
set -e

#Set environment variables
: ${KEYSTONE_MEM:=12g}
export KEYSTONE_MEM

KEYSTONE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../..
: ${EXAMPLE_DATA_DIR:=$KEYSTONE_DIR/example_data}


#Get the data and copy to HDFS if necessary.
if [ ! -f $EXAMPLE_DATA_DIR/VOCtrainval_06-Nov-2007.tar ]; then
    wget -O $EXAMPLE_DATA_DIR/VOCtrainval_06-Nov-2007.tar http://s3-us-west-2.amazonaws.com/voc-data/VOCtrainval_06-Nov-2007.tar
fi

if [ ! -f $EXAMPLE_DATA_DIR/VOCtest_06-Nov-2007.tar ]; then
    wget -O $EXAMPLE_DATA_DIR/VOCtest_06-Nov-2007.tar http://s3-us-west-2.amazonaws.com/voc-data/VOCtest_06-Nov-2007.tar
fi

#Run the pipeline
$KEYSTONE_DIR/bin/run-pipeline.sh \
  keystoneml.pipelines.images.voc.VOCSIFTFisher \
  --trainLocation $EXAMPLE_DATA_DIR/VOCtrainval_06-Nov-2007.tar \
  --testLocation $EXAMPLE_DATA_DIR/VOCtest_06-Nov-2007.tar \
  --labelPath $KEYSTONE_DIR/src/test/resources/images/voclabels.csv \
  --numParts 200
