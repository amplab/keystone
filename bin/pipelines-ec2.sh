#!/bin/bash

if [ -z "$SPARK_EC2_DIR" ] || [ ! -f "$SPARK_EC2_DIR"/spark-ec2 ]; then
  echo "SPARK_EC2_DIR is not set correctly, please set SPARK_EC2_DIR to be <your_spark_clone>/ec2"
  exit 1
fi

$SPARK_EC2_DIR/spark-ec2 \
  --hadoop-major-version=2 \
  --spark-version=1.3.1 \
  --spark-ec2-git-repo=https://github.com/shivaram/spark-ec2 \
  --spark-ec2-git-branch=keystone \
  --copy-aws-credentials \
  $@
