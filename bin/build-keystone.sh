#!/bin/bash

pushd /root/keystone
./sbt/sbt assembly

/root/spark-ec2/copy-dir /root/keystone

popd
