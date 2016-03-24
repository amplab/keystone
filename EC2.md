# Running KeystoneML on EC2

To run KeystoneML on EC2 you can use the
[spark-ec2](http://spark.apache.org/docs/latest/ec2-scripts.html) scripts.

## Getting spark-ec2

As the KeystoneML scripts require a recent version of spark-ec2, it is
recommended that you clone the spark-ec2 master branch for this. You can do this with
```
git clone https://github.com/amplab/spark-ec2.git
``` 

## Launching a Cluster

You can now use the `bin/keystone-ec2.sh` to launch a cluster with KeystoneML pre-installed.
To do that you can run a command which looks like 

```
SPARK_EC2_DIR=<path_to_your_spark-ec2> ./bin/keystone-ec2.sh \
  -s 4 \
  -t r3.4xlarge \
  -i <key-file> \
  -k <key-name> \
  launch keystone-test-cluster
```

The above command launches 4 slaves and 1 master machine of type r3.4xlarge.
Note that you can pass in any spark-ec2 options (like spot-prices etc.) to this script.

## Running KeystoneML on the cluster

Once the cluster launch finishes you can login to the master node and the KeystoneML
repository should be present in `/root/keystone`.
