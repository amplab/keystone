#!/bin/bash
set -e

#Set environment variables
: ${KEYSTONE_MEM:=4g}
export KEYSTONE_MEM

: ${NUM_PARTS:=256}
: ${NGRAMS:=2}
: ${COMMON_FEATURES:=1000}

KEYSTONE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../..
: ${EXAMPLE_DATA_DIR:=$KEYSTONE_DIR/example_data}

if [ ! -d $EXAMPLE_DATA_DIR ]; then
    mkdir $EXAMPLE_DATA_DIR
fi


#Download 20 Newsgroups data if necessary.
if [ ! -f $EXAMPLE_DATA_DIR/20news-bydate.tar.gz ]; then
    wget -O $EXAMPLE_DATA_DIR/20news-bydate.tar.gz http://qwone.com/~jason/20Newsgroups/20news-bydate.tar.gz
    tar zxvf $EXAMPLE_DATA_DIR/20news-bydate.tar.gz -C $EXAMPLE_DATA_DIR
fi

#Run pipeline.
$KEYSTONE_DIR/bin/run-pipeline.sh \
    keystoneml.pipelines.text.NewsgroupsPipeline \
    --trainLocation $EXAMPLE_DATA_DIR/20news-bydate-train \
    --testLocation $EXAMPLE_DATA_DIR/20news-bydate-test \
    --nGrams $NGRAMS \
    --commonFeatures $COMMON_FEATURES
