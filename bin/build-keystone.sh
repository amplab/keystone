#!/bin/bash

pushd /root/keystone
./sbt/sbt assembly

/root/spark-ec2/copy-dir /root/keystone/target/scala-2.10/keystone-assembly-0.1.jar

popd
