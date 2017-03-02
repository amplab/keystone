#!/bin/bash
set -e

#Set environment variables
: ${KEYSTONE_MEM:=4g}
export KEYSTONE_MEM

KEYSTONE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../..
: ${HADOOP_EXAMPLE_DATA_DIR:=/example_data}


#Get the data and copy to HDFS if necessary.
if [ ! hadoop fs -test -e $HADOOP_EXAMPLE_DATA_DIR/VOCtrainval_06-Nov-2007.tar ]; then
    wget -O $TMPDIR/VOCtrainval_06-Nov-2007.tar https://s3-us-west-2.amazonaws.com/voc-data/VOCtrainval_06-Nov-2007.tar
    hadoop fs -copyFromLocal VOCtrainval_06-Nov-2007.tar $HADOOP_EXAMPLE_DATA_DIR
    rm -f $TMPDIR/VOCtrainval_06-Nov-2007.tar
fi

if [ ! hadoop fs -test -e $HADOOP_EXAMPLE_DATA_DIR/VOCtest_06-Nov-2007.tar ]; then
    wget -O $TMPDIR/VOCtest_06-Nov-2007.tar https://s3-us-west-2.amazonaws.com/voc-data/VOCtest_06-Nov-2007.tar
    hadoop fs -copyFromLocal VOCtest_06-Nov-2007.tar $HADOOP_EXAMPLE_DATA_DIR
    rm -f $TMPDIR/VOCtest_06-Nov-2007.tar
fi

#Run the pipeline
$KEYSTONE_DIR/bin/run-pipeline.sh \
  keystoneml.pipelines.images.voc.VOCSIFTFisher \
  --trainLocation $HADOOP_EXAMPLE_DATA_DIR/VOCtrainval_06-Nov-2007.tar \
  --testLocation $HADOOP_EXAMPLE_DATA_DIR/VOCtest_06-Nov-2007.tar \
  --labelPath $KEYSTONE_DIR/src/test/resources/images/voclabels.csv \
  --numParts 200
