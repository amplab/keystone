# Running KeystoneML on EC2

To run KeystoneML on EC2 you can use the
[spark-ec2](http://spark.apache.org/docs/latest/ec2-scripts.html)

## Getting spark-ec2

As KeystoneML requires a recent version of spark-ec2, it is recommended that you clone
the Spark source code master branch for this. You can do this with a command
```
git clone https://github.com/apache/spark.git
``` 

## Launching a Cluster

You can now use the `bin/keystone-ec2.sh` to launch a cluster with KeystonML pre-installed.
To do that you can run a command which looks like 

```
SPARK_EC2_DIR=<path_to_your_spark>/ec2 ./bin/keystone-ec2.sh \
  -s 4 \
  -t r3.4xlarge \
  --spot-price 0.8 \
  -i <key-file> \
  -k <key-name> \
  launch keystone-test-cluster
```

Note that you can pass in any spark-ec2 options to this script.
